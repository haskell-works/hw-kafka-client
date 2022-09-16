{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE LambdaCase                 #-}

-----------------------------------------------------------------------------
-- |
-- Module to produce messages to Kafka topics.
-- 
-- Here's an example of code to produce messages to a topic:
-- 
-- @
-- import Control.Exception (bracket)
-- import Control.Monad (forM_)
-- import Data.ByteString (ByteString)
-- import Kafka.Producer
-- 
-- -- Global producer properties
-- producerProps :: 'ProducerProperties'
-- producerProps = 'brokersList' ["localhost:9092"]
--              <> 'logLevel' 'KafkaLogDebug'
-- 
-- -- Topic to send messages to
-- targetTopic :: 'TopicName'
-- targetTopic = 'TopicName' "kafka-client-example-topic"
-- 
-- -- Run an example
-- runProducerExample :: IO ()
-- runProducerExample =
--     bracket mkProducer clProducer runHandler >>= print
--     where
--       mkProducer = 'newProducer' producerProps
--       clProducer (Left _)     = pure ()
--       clProducer (Right prod) = 'closeProducer' prod
--       runHandler (Left err)   = pure $ Left err
--       runHandler (Right prod) = sendMessages prod
-- 
-- -- Example sending 2 messages and printing the response from Kafka
-- sendMessages :: 'KafkaProducer' -> IO (Either 'KafkaError' ())
-- sendMessages prod = do
--   err1 <- 'produceMessage' prod (mkMessage Nothing (Just "test from producer") )
--   forM_ err1 print
-- 
--   err2 <- 'produceMessage' prod (mkMessage (Just "key") (Just "test from producer (with key)"))
--   forM_ err2 print
-- 
--   pure $ Right ()
-- 
-- mkMessage :: Maybe ByteString -> Maybe ByteString -> 'ProducerRecord'
-- mkMessage k v = 'ProducerRecord'
--                   { 'prTopic' = targetTopic
--                   , 'prPartition' = 'UnassignedPartition'
--                   , 'prKey' = k
--                   , 'prValue' = v
--                   }
-- @
-----------------------------------------------------------------------------
module Kafka.Producer
( KafkaProducer
, module X
, runProducer
, newProducer
, produceMessage
, produceMessage'
, flushProducer
, closeProducer
, RdKafkaRespErrT (..)

, initTransactions
, beginTransaction
, commitTransaction
, abortTransaction

, produceMessageTxError
, commitOffsetMessageTransaction
, commitAllOffsetsTransaction

, TxError
, getKafkaError
, kafkaErrorIsFatal
, kafkaErrorIsRetriable
, kafkaErrorTxnRequiresAbort
)
where

import           Control.Exception        (bracket)
import           Control.Monad            (forM_)
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Text                as Text
import           Foreign.C.String         (withCString)
import           Foreign.ForeignPtr       (withForeignPtr, newForeignPtr_)
import           Foreign.Marshal.Utils    (withMany)
import           Foreign.Ptr              (Ptr, nullPtr, plusPtr)
import           Foreign.StablePtr        (newStablePtr, castStablePtrToPtr)
import           Kafka.Internal.RdKafka   (RdKafkaRespErrT (..), RdKafkaTypeT (..), RdKafkaVuT(..), RdKafkaErrorTPtr, RdKafkaErrorTPtr, rdKafkaErrorDestroy, rdKafkaErrorIsFatal, rdKafkaErrorIsRetriable, rdKafkaErrorTxnRequiresAbort, rdKafkaErrorCode, rdKafkaInitTransactions, rdKafkaBeginTransaction, rdKafkaCommitTransaction, rdKafkaAbortTransaction, rdKafkaSendOffsetsToTransaction, newRdKafkaT, rdKafkaErrorCode, rdKafkaErrorDestroy, rdKafkaOutqLen, rdKafkaMessageProduceVa, rdKafkaSetLogLevel)
import           Kafka.Internal.Setup     (Kafka (..), KafkaConf (..), KafkaProps (..), TopicProps (..), getRdKafka, kafkaConf, topicConf, Callback(..))
import           Kafka.Internal.Shared    (pollEvents)
import           Kafka.Producer.Convert   (copyMsgFlags, handleProduceErrT, producePartitionCInt)
import           Kafka.Producer.Types     (KafkaProducer (..))
import           Kafka.Consumer.Convert   (toNativeTopicPartitionList, topicPartitionFromMessageForCommit)
import           Kafka.Consumer.Types     (KafkaConsumer (..), ConsumerRecord)

import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types              as X hiding (KafkaProducer)
import Kafka.Types                       as X

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- to send messages to Kafka.
{-# DEPRECATED runProducer "Use 'newProducer'/'closeProducer' instead" #-}
runProducer :: ProducerProperties
            -> (KafkaProducer -> IO (Either KafkaError a))
            -> IO (Either KafkaError a)
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _)     = return ()
    clProducer (Right prod) = closeProducer prod

    runHandler (Left err)   = return $ Left err
    runHandler (Right prod) = f prod

-- | Creates a new kafka producer
-- A newly created producer must be closed with 'closeProducer' function.
newProducer :: MonadIO m => ProducerProperties -> m (Either KafkaError KafkaProducer)
newProducer pps = liftIO $ do
  kc@(KafkaConf kc' _ _) <- kafkaConf (KafkaProps $ (ppKafkaProps pps))
  tc <- topicConf (TopicProps $ (ppTopicProps pps))

  -- add default delivery report callback
  let Callback setDeliveryCallback = deliveryCallback (const mempty)
  setDeliveryCallback kc

  -- set callbacks
  forM_ (ppCallbacks pps) (\(Callback setCb) -> setCb kc)

  mbKafka <- newRdKafkaT RdKafkaProducer kc'
  case mbKafka of
    Left err    -> return . Left $ KafkaError err
    Right kafka -> do
      forM_ (ppLogLevel pps) (rdKafkaSetLogLevel kafka . fromEnum)
      let prod = KafkaProducer (Kafka kafka) kc tc
      return (Right prod)

-- | Sends a single message.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessage :: MonadIO m
               => KafkaProducer
               -> ProducerRecord
               -> m (Maybe KafkaError)
produceMessage kp m = produceMessage' kp m (pure . mempty) >>= adjustRes
  where
    adjustRes = \case
      Right () -> pure Nothing
      Left (ImmediateError err) -> pure (Just err)

-- | Sends a single message with a registered callback.
--
--   The callback can be a long running process, as it is forked by the thread
--   that handles the delivery reports.
produceMessage' :: MonadIO m
                => KafkaProducer
                -> ProducerRecord
                -> (DeliveryReport -> IO ())
                -> m (Either ImmediateError ())
produceMessage' p msg cb = liftIO $ do
  code <- bracket (produceMessageErrT p msg cb) rdKafkaErrorDestroy rdKafkaErrorCode
  res  <- handleProduceErrT code
  pure $ case res of
    Just err -> Left . ImmediateError $ err
    Nothing -> Right ()

-- | Closes the producer.
-- Will wait until the outbound queue is drained before returning the control.
closeProducer :: MonadIO m => KafkaProducer -> m ()
closeProducer = flushProducer

-- | Drains the outbound queue for a producer.
--  This function is also called automatically when the producer is closed
-- with 'closeProducer' to ensure that all queued messages make it to Kafka.
flushProducer :: MonadIO m => KafkaProducer -> m ()
flushProducer kp = liftIO $ do
    pollEvents kp (Just $ Timeout 100)
    l <- outboundQueueLength (kpKafkaPtr kp)
    if (l == 0)
      then pollEvents kp (Just $ Timeout 0) -- to be sure that all the delivery reports are fired
      else flushProducer kp

------------------------------------------------------------------------------------
-- Tx API

data TxError = TxError 
  { txErrorKafka       :: !KafkaError 
  , txErrorFatal       :: !Bool
  , txErrorRetriable   :: !Bool
  , txErrorTxnReqAbort :: !Bool
  } 

-- | Initialises Kafka for transactions 
initTransactions :: MonadIO m 
                 => KafkaProducer 
                 -> Timeout 
                 -> m (Maybe TxError)
initTransactions p (Timeout to) = liftIO $ rdKafkaInitTransactions (getRdKafka p) to >>= toTxError

-- | Begins a new transaction
beginTransaction :: MonadIO m 
                 => KafkaProducer 
                 -> m (Maybe TxError)
beginTransaction p = liftIO $ rdKafkaBeginTransaction (getRdKafka p) >>= toTxError

-- | Commits an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitTransaction :: MonadIO m 
                  => KafkaProducer 
                  -> Timeout 
                  -> m (Maybe TxError)
commitTransaction p (Timeout to) = liftIO $ rdKafkaCommitTransaction (getRdKafka p) to >>= toTxError

-- | Aborts an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
abortTransaction :: MonadIO m 
                 => KafkaProducer 
                 -> Timeout 
                 -> m (Maybe TxError)
abortTransaction p (Timeout to) = liftIO $ do rdKafkaAbortTransaction (getRdKafka p) to >>= toTxError

-- | Commits the message's offset in the current transaction
--    Similar to Kafka.Consumer.commitOffsetMessage but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitOffsetMessageTransaction :: MonadIO m 
                               => KafkaProducer 
                               -> KafkaConsumer 
                               -> ConsumerRecord k v
                               -> Timeout 
                               -> m (Maybe TxError)
commitOffsetMessageTransaction p c m (Timeout to) = liftIO $ do
  tps <- toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

-- | Commit offsets for all currently assigned partitions in the current transaction
--    Similar to Kafka.Consumer.commitAllOffsets but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitAllOffsetsTransaction :: MonadIO m 
                            => KafkaProducer 
                            -> KafkaConsumer 
                            -> Timeout 
                            -> m (Maybe TxError)
commitAllOffsetsTransaction p c (Timeout to) = liftIO $ do
  tps <- newForeignPtr_ nullPtr
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

-- | This is the same as Kafka.Producer.produceMessage but returns a TxError
produceMessageTxError :: MonadIO m
                      => KafkaProducer
                      -> ProducerRecord
                      -> m (Maybe TxError)
produceMessageTxError p pr = liftIO $ produceMessageErrT p pr (pure . mempty) >>= toTxError

getKafkaError :: TxError -> KafkaError
getKafkaError = txErrorKafka

kafkaErrorIsFatal :: TxError -> Bool
kafkaErrorIsFatal = txErrorFatal

kafkaErrorIsRetriable :: TxError -> Bool
kafkaErrorIsRetriable = txErrorRetriable

kafkaErrorTxnRequiresAbort :: TxError -> Bool
kafkaErrorTxnRequiresAbort = txErrorTxnReqAbort

----------------------------------------------------------------------------------------------------
-- Implementation detail, used internally
toTxError :: RdKafkaErrorTPtr -> IO (Maybe TxError)
toTxError errPtr = do
  ret <- rdKafkaErrorCode errPtr >>= handleProduceErrT
  case ret of
    Nothing -> pure Nothing
    Just ke -> do
      fatal     <- rdKafkaErrorIsFatal errPtr
      retriable <- rdKafkaErrorIsRetriable errPtr
      reqAbort  <- rdKafkaErrorTxnRequiresAbort errPtr
      -- NOTE: don't forget to free error structure, otherwise we are leaking memory!
      rdKafkaErrorDestroy errPtr
      pure $ Just $ TxError 
        { txErrorKafka       = ke
        , txErrorFatal       = fatal
        , txErrorRetriable   = retriable
        , txErrorTxnReqAbort = reqAbort
        }


-- used internally
produceMessageErrT :: MonadIO m
                   => KafkaProducer
                   -> ProducerRecord
                   -> (DeliveryReport -> IO ())
                   -> m RdKafkaErrorTPtr
produceMessageErrT kp@(KafkaProducer (Kafka k) _ _) msg cb = liftIO $
    fireCallbacks >> produceIt
  where
    fireCallbacks =
      pollEvents kp . Just . Timeout $ 0

    produceIt =
      withBS (prValue msg) $ \payloadPtr payloadLength ->
        withBS (prKey msg) $ \keyPtr keyLength ->
          withHeaders (prHeaders msg) $ \hdrs ->
            withCString (Text.unpack . unTopicName . prTopic $ msg) $ \topicName -> do
              callbackPtr <- newStablePtr cb
              let opts = [
                      Topic'RdKafkaVu topicName
                    , Partition'RdKafkaVu . producePartitionCInt . prPartition $ msg
                    , MsgFlags'RdKafkaVu (fromIntegral copyMsgFlags)
                    , Value'RdKafkaVu payloadPtr (fromIntegral payloadLength)
                    , Key'RdKafkaVu keyPtr (fromIntegral keyLength)
                    , Opaque'RdKafkaVu (castStablePtrToPtr callbackPtr)
                    ]

              rdKafkaMessageProduceVa k (hdrs ++ opts)

withHeaders :: Headers -> ([RdKafkaVuT] -> IO a) -> IO a
withHeaders hds = withMany allocHeader (headersToList hds)
  where
    allocHeader (nm, val) f = 
      BS.useAsCString nm $ \cnm ->
          withBS (Just val) $ \vp vl ->
            f $ Header'RdKafkaVu cnm vp (fromIntegral vl)

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka k) = rdKafkaOutqLen k
