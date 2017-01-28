module Kafka.Producer
( module X
, runProducer
, newProducer
, produceMessage
, drainOutQueue
, closeProducer
, RDE.RdKafkaRespErrT (..)
)
where

import           Control.Exception
import           Control.Monad
import qualified Data.Map as M
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Internal        as BSI
import           Foreign                         hiding (void)
import           Kafka
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Producer.Convert
import           Kafka.Producer.Types
import           Kafka.Producer.ProducerProperties
-- import           Data.Function (on)
-- import           Data.List (sortBy, groupBy)
-- import           Data.Ord (comparing)

import qualified Kafka.Internal.RdKafkaEnum      as RDE

import qualified Kafka.Producer.Types as X
import qualified Kafka.Producer.ProducerProperties as X

runProducer :: ProducerProperties
            -> (KafkaProducer -> IO (Either KafkaError a))
            -> IO (Either KafkaError a)
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _) = return ()
    clProducer (Right prod) = closeProducer prod

    runHandler (Left err) = return $ Left err
    runHandler (Right prod) = f prod

-- | Creates a new kafka producer
newProducer :: ProducerProperties -> IO (Either KafkaError KafkaProducer)
newProducer (ProducerProperties kp tp) = do
  (KafkaConf kc) <- kafkaConf (KafkaProps $ M.toList kp)
  (TopicConf tc) <- topicConf (TopicProps $ M.toList tp)
  mbKafka        <- newRdKafkaT RdKafkaProducer kc
  return $ case mbKafka of
    Left err    -> Left $ KafkaError err
    Right kafka -> Right $ KafkaProducer kafka kc tc


-- | Produce a single unkeyed message to either a random partition or specified partition. Since
-- librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'drainOutQueue' to wait for queue to empty.
produceMessage :: KafkaProducer
               -> ProducerRecord    -- ^ the message to enqueue. This function is undefined for keyed messages.
               -> IO (Maybe KafkaError)  -- ^ 'Nothing' on success, error if something went wrong.
produceMessage (KafkaProducer k _ tc) m =
  bracket mkTopic clTopic withTopic
    where
      (TopicName tn, key, partition, payload) = keyAndPayload m
      mkTopic = newUnmanagedRdKafkaTopicT k tn tc

      clTopic (Left _) = return ()
      clTopic (Right t) = destroyUnmanagedRdKafkaTopic t

      withTopic (Left err) = return . Just . KafkaError $ err
      withTopic (Right t) =
        withBS (Just payload) $ \payloadPtr payloadLength ->
          withBS key $ \keyPtr keyLength ->
            handleProduceErr =<<
              rdKafkaProduce t (producePartitionCInt partition)
                copyMsgFlags payloadPtr (fromIntegral payloadLength)
                keyPtr (fromIntegral keyLength) nullPtr
--
-- let (topic, key, partition, payload) = keyAndPayload m
-- in withBS (Just payload) $ \payloadPtr payloadLength ->
--         withBS key $ \keyPtr keyLength ->
--           handleProduceErr =<<
--             rdKafkaProduce t (producePartitionCInt partition)
--               copyMsgFlags payloadPtr (fromIntegral payloadLength)
--               keyPtr (fromIntegral keyLength) nullPtr
--
-- | Produce a batch of messages. Since librdkafka is backed by a queue, this function can return
-- before messages are sent. See 'drainOutQueue' to wait for the queue to be empty.
-- produceMessageBatch :: KafkaTopic  -- ^ topic pointer
--                     -> [ProducerRecord] -- ^ list of messages to enqueue.
--                     -> IO [(ProducerRecord, KafkaError)] -- list of failed messages with their errors. This will be empty on success.
-- produceMessageBatch (KafkaTopic t _ _) pms =
--   concat <$> mapM sendBatch (partBatches pms)
--   where
--     partBatches msgs = (\x -> (pmPartition (head x), x)) <$> batches msgs
--     batches = groupBy ((==) `on` pmPartition) . sortBy (comparing pmPartition)
--     sendBatch (part, ms) = do
--       msgs <- forM ms toNativeMessage
--       let msgsCount = length msgs
--       withArray msgs $ \batchPtr -> do
--         batchPtrF <- newForeignPtr_ batchPtr
--         numRet    <- rdKafkaProduceBatch t (producePartitionCInt part) copyMsgFlags batchPtrF msgsCount
--         if numRet == msgsCount then return []
--         else do
--           errs <- mapM (return . err'RdKafkaMessageT <=< peekElemOff batchPtr)
--                        [0..(fromIntegral $ msgsCount - 1)]
--           return [(m, KafkaResponseError e) | (m, e) <- zip pms errs, e /= RdKafkaRespErrNoError]
--       where
--           toNativeMessage msg =
--               let (key, partition, payload) = keyAndPayload msg
--               in  withBS (Just payload) $ \payloadPtr payloadLength ->
--                       withBS key $ \keyPtr keyLength ->
--                           withForeignPtr t $ \ptrTopic ->
--                               return RdKafkaMessageT
--                                 { err'RdKafkaMessageT       = RdKafkaRespErrNoError
--                                 , topic'RdKafkaMessageT     = ptrTopic
--                                 , partition'RdKafkaMessageT = producePartitionInt partition
--                                 , len'RdKafkaMessageT       = payloadLength
--                                 , payload'RdKafkaMessageT   = payloadPtr
--                                 , offset'RdKafkaMessageT    = 0
--                                 , keyLen'RdKafkaMessageT    = keyLength
--                                 , key'RdKafkaMessageT       = keyPtr
--                                 }

closeProducer :: KafkaProducer -> IO ()
closeProducer = drainOutQueue

-- | Drains the outbound queue for a producer. This function is called automatically at the end of
-- 'runKafkaProducer' or 'runKafkaProducerConf' and usually doesn't need to be called directly.
drainOutQueue :: KafkaProducer -> IO ()
drainOutQueue kp@(KafkaProducer k _ _) = do
    pollEvents k 100
    l <- outboundQueueLength k
    unless (l == 0) $ drainOutQueue kp

------------------------------------------------------------------------------------
keyAndPayload :: ProducerRecord -> (TopicName, Maybe BS.ByteString, ProducePartition, BS.ByteString)
keyAndPayload (ProducerRecord topic partition payload) = (topic, Nothing, partition, payload)
keyAndPayload (KeyedProducerRecord topic key partition payload) = (topic, Just key, partition, payload)

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

pollEvents :: RdKafkaTPtr -> Int -> IO ()
pollEvents kPtr timeout = void (rdKafkaPoll kPtr timeout)

outboundQueueLength :: RdKafkaTPtr -> IO Int
outboundQueueLength = rdKafkaOutqLen
