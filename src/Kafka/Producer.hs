{-# LANGUAGE TupleSections #-}
module Kafka.Producer
( module X
, runProducer
, newProducer
, produceMessage, produceMessageBatch
, flushProducer
, closeProducer
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow            ((&&&))
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Function            (on)
import           Data.List                (groupBy, sortBy)
import qualified Data.Map                 as M
import           Data.Ord                 (comparing)
import           Foreign                  hiding (void)
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Producer.Convert

import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types              as X
import Kafka.Types                       as X

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- or/and 'produceMessageBatch' to send messages to Kafka.
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
newProducer (ProducerProperties kp tp ll cbs) = liftIO $ do
  kc@(KafkaConf kc') <- kafkaConf (KafkaProps $ M.toList kp)
  tc <- topicConf (TopicProps $ M.toList tp)

  -- set callbacks
  forM_ cbs (\setCb -> setCb kc)

  mbKafka <- newRdKafkaT RdKafkaProducer kc'
  case mbKafka of
    Left err    -> return . Left $ KafkaError err
    Right kafka -> do
      forM_ ll (rdKafkaSetLogLevel kafka . fromEnum)
      return .Right $ KafkaProducer (Kafka kafka) kc tc

-- | Sends a single message.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessage :: MonadIO m
               => KafkaProducer
               -> ProducerRecord
               -> m (Maybe KafkaError)
produceMessage (KafkaProducer (Kafka k) _ (TopicConf tc)) m = liftIO $
  bracket (mkTopic $ prTopic m) clTopic withTopic
    where
      mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k tn (Just tc)

      clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

      withTopic (Left err) = return . Just . KafkaError $ err
      withTopic (Right t) =
        withBS (prValue m) $ \payloadPtr payloadLength ->
          withBS (prKey m) $ \keyPtr keyLength ->
            handleProduceErr =<<
              rdKafkaProduce t (producePartitionCInt (prPartition m))
                copyMsgFlags payloadPtr (fromIntegral payloadLength)
                keyPtr (fromIntegral keyLength) nullPtr


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
produceMessageBatch (KafkaProducer (Kafka k) _ (TopicConf tc)) messages = liftIO $
  concat <$> forM (mkBatches messages) sendBatch
  where
    mkSortKey = prTopic &&& prPartition
    mkBatches = groupBy ((==) `on` mkSortKey) . sortBy (comparing mkSortKey)

    mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k tn (Just tc)

    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

    sendBatch []    = return []
    sendBatch batch = bracket (mkTopic $ prTopic (head batch)) clTopic (withTopic batch)

    withTopic ms (Left err) = return $ (, KafkaError err) <$> ms
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
            }

-- | Closes the producer.
-- Will wait until the outbound queue is drained before returning the control.
closeProducer :: MonadIO m => KafkaProducer -> m ()
closeProducer = flushProducer

-- | Drains the outbound queue for a producer.
--  This function is also called automatically when the producer is closed
-- with 'closeProducer' to ensure that all queued messages make it to Kafka.
flushProducer :: MonadIO m => KafkaProducer -> m ()
flushProducer kp@(KafkaProducer (Kafka k) _ _) = liftIO $ do
    pollEvents k 100
    l <- outboundQueueLength k
    unless (l == 0) $ flushProducer kp

------------------------------------------------------------------------------------

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

pollEvents :: RdKafkaTPtr -> Int -> IO ()
pollEvents kPtr timeout = void (rdKafkaPoll kPtr timeout)

outboundQueueLength :: RdKafkaTPtr -> IO Int
outboundQueueLength = rdKafkaOutqLen
