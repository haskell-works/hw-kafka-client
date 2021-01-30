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
, produceMessage, produceMessageBatch
, produceMessage'
, flushProducer
, closeProducer
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow            ((&&&))
import           Control.Exception        (bracket)
import           Control.Monad            (forM, forM_, (<=<))
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Function            (on)
import           Data.List                (groupBy, sortBy)
import           Data.Ord                 (comparing)
import qualified Data.Text                as Text
import           Foreign.ForeignPtr       (newForeignPtr_, withForeignPtr)
import           Foreign.Marshal.Array    (withArrayLen)
import           Foreign.Ptr              (Ptr, nullPtr, plusPtr)
import           Foreign.Storable         (Storable (..))
import           Foreign.StablePtr        (newStablePtr, castStablePtrToPtr)
import           Kafka.Internal.RdKafka   (RdKafkaMessageT (..), RdKafkaRespErrT (..), RdKafkaTypeT (..), destroyUnmanagedRdKafkaTopic, newRdKafkaT, newUnmanagedRdKafkaTopicT, rdKafkaOutqLen, rdKafkaProduce, rdKafkaProduceBatch, rdKafkaSetLogLevel)
import           Kafka.Internal.Setup     (Kafka (..), KafkaConf (..), KafkaProps (..), TopicConf (..), TopicProps (..), kafkaConf, topicConf, Callback(..))
import           Kafka.Internal.Shared    (pollEvents)
import           Kafka.Producer.Convert   (copyMsgFlags, handleProduceErr', producePartitionCInt, producePartitionInt)
import           Kafka.Producer.Types     (KafkaProducer (..))

import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types              as X hiding (KafkaProducer)
import Kafka.Types                       as X

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- or/and 'produceMessageBatch' to send messages to Kafka.
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
--
produceMessage' :: MonadIO m
                => KafkaProducer
                -> ProducerRecord
                -> (DeliveryReport -> IO ())
                -> m (Either ImmediateError ())
produceMessage' kp@(KafkaProducer (Kafka k) _ (TopicConf tc)) msg cb = liftIO $
  fireCallbacks >> bracket (mkTopic . prTopic $ msg) closeTopic withTopic
  where
    fireCallbacks =
      pollEvents kp . Just . Timeout $ 0

    mkTopic (TopicName tn) =
      newUnmanagedRdKafkaTopicT k (Text.unpack tn) (Just tc)

    closeTopic = either mempty destroyUnmanagedRdKafkaTopic

    withTopic (Left err) = return . Left . ImmediateError . KafkaError . Text.pack $ err
    withTopic (Right topic) =
      withBS (prValue msg) $ \payloadPtr payloadLength ->
        withBS (prKey msg) $ \keyPtr keyLength -> do
          callbackPtr <- newStablePtr cb
          res <- handleProduceErr' =<< rdKafkaProduce
            topic
            (producePartitionCInt (prPartition msg))
            copyMsgFlags
            payloadPtr
            (fromIntegral payloadLength)
            keyPtr
            (fromIntegral keyLength)
            (castStablePtrToPtr callbackPtr)

          pure $ case res of
            Left err -> Left . ImmediateError $ err
            Right () -> Right ()

-- | Sends a batch of messages.
-- Returns a list of messages which it was unable to send with corresponding errors.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessageBatch :: MonadIO m
                    => KafkaProducer
                    -> [ProducerRecord]
                    -> m [(ProducerRecord, KafkaError)]
                    -- ^ An empty list when the operation is successful,
                    -- otherwise a list of "failed" messages with corresponsing errors.
produceMessageBatch kp@(KafkaProducer (Kafka k) _ (TopicConf tc)) messages = liftIO $ do
  pollEvents kp (Just $ Timeout 0) -- fire callbacks if any exist (handle delivery reports)
  concat <$> forM (mkBatches messages) sendBatch
  where
    mkSortKey = prTopic &&& prPartition
    mkBatches = groupBy ((==) `on` mkSortKey) . sortBy (comparing mkSortKey)

    mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k (Text.unpack tn) (Just tc)

    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

    sendBatch []    = return []
    sendBatch batch = bracket (mkTopic $ prTopic (head batch)) clTopic (withTopic batch)

    withTopic ms (Left err) = return $ (, KafkaError (Text.pack err)) <$> ms
    withTopic ms (Right t) = do
      let (partInt, partCInt) = (producePartitionInt &&& producePartitionCInt) $ prPartition (head ms)
      withForeignPtr t $ \topicPtr -> do
        nativeMs <- forM ms (toNativeMessage topicPtr partInt)
        withArrayLen nativeMs $ \len batchPtr -> do
          batchPtrF <- newForeignPtr_ batchPtr
          numRet    <- rdKafkaProduceBatch t partCInt copyMsgFlags batchPtrF len
          if numRet == len then return []
          else do
            errs <- mapM (return . err'RdKafkaMessageT <=< peekElemOff batchPtr)
                         [0..(fromIntegral $ len - 1)]
            return [(m, KafkaResponseError e) | (m, e) <- zip messages errs, e /= RdKafkaRespErrNoError]

    toNativeMessage t p m =
      withBS (prValue m) $ \payloadPtr payloadLength ->
        withBS (prKey m) $ \keyPtr keyLength ->
          return RdKafkaMessageT
            { err'RdKafkaMessageT       = RdKafkaRespErrNoError
            , topic'RdKafkaMessageT     = t
            , partition'RdKafkaMessageT = p
            , len'RdKafkaMessageT       = payloadLength
            , payload'RdKafkaMessageT   = payloadPtr
            , offset'RdKafkaMessageT    = 0
            , keyLen'RdKafkaMessageT    = keyLength
            , key'RdKafkaMessageT       = keyPtr
            , opaque'RdKafkaMessageT    = nullPtr
            }

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

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka k) = rdKafkaOutqLen k
