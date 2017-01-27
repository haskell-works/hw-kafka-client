module Kafka.Producer
( module X
, runProducer
, newProducer
, produceMessage
, produceMessageBatch
, drainOutQueue
, closeProducer
, IS.newKafkaTopic
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
import           Data.Function (on)
import           Data.List (sortBy, groupBy)
import           Data.Ord (comparing)

import qualified Kafka.Internal.RdKafkaEnum      as RDE
import qualified Kafka.Internal.Setup            as IS

import qualified Kafka.Producer.Types as X
import qualified Kafka.Producer.ProducerProperties as X

runProducer :: ProducerProperties
            -> (Kafka -> IO a)
            -> IO a
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer props
    clProducer = drainOutQueue >> closeProducer
    runHandler = f

-- | Creates a new kafka producer
newProducer :: ProducerProperties -> IO Kafka
newProducer (ProducerProperties props) = do
  conf  <- kafkaConf (KafkaProps $ M.toList props)
  newKafkaPtr RdKafkaProducer conf

-- | Produce a single unkeyed message to either a random partition or specified partition. Since
-- librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'drainOutQueue' to wait for queue to empty.
produceMessage :: KafkaTopic             -- ^ target topic
               -> ProducerRecord    -- ^ the message to enqueue. This function is undefined for keyed messages.
               -> IO (Maybe KafkaError)  -- ^ 'Nothing' on success, error if something went wrong.
produceMessage (KafkaTopic t _ _) m =
    let (key, partition, payload) = keyAndPayload m
    in  withBS (Just payload) $ \payloadPtr payloadLength ->
            withBS key $ \keyPtr keyLength ->
              handleProduceErr =<<
                rdKafkaProduce t (producePartitionCInt partition)
                  copyMsgFlags payloadPtr (fromIntegral payloadLength)
                  keyPtr (fromIntegral keyLength) nullPtr

-- | Produce a batch of messages. Since librdkafka is backed by a queue, this function can return
-- before messages are sent. See 'drainOutQueue' to wait for the queue to be empty.
produceMessageBatch :: KafkaTopic  -- ^ topic pointer
                    -> [ProducerRecord] -- ^ list of messages to enqueue.
                    -> IO [(ProducerRecord, KafkaError)] -- list of failed messages with their errors. This will be empty on success.
produceMessageBatch (KafkaTopic t _ _) pms =
  concat <$> mapM sendBatch (partBatches pms)
  where
    partBatches msgs = (\x -> (pmPartition (head x), x)) <$> batches msgs
    batches = groupBy ((==) `on` pmPartition) . sortBy (comparing pmPartition)
    sendBatch (part, ms) = do
      msgs <- forM ms toNativeMessage
      let msgsCount = length msgs
      withArray msgs $ \batchPtr -> do
        batchPtrF <- newForeignPtr_ batchPtr
        numRet    <- rdKafkaProduceBatch t (producePartitionCInt part) copyMsgFlags batchPtrF msgsCount
        if numRet == msgsCount then return []
        else do
          errs <- mapM (return . err'RdKafkaMessageT <=< peekElemOff batchPtr)
                       [0..(fromIntegral $ msgsCount - 1)]
          return [(m, KafkaResponseError e) | (m, e) <- zip pms errs, e /= RdKafkaRespErrNoError]
      where
          toNativeMessage msg =
              let (key, partition, payload) = keyAndPayload msg
              in  withBS (Just payload) $ \payloadPtr payloadLength ->
                      withBS key $ \keyPtr keyLength ->
                          withForeignPtr t $ \ptrTopic ->
                              return RdKafkaMessageT
                                { err'RdKafkaMessageT       = RdKafkaRespErrNoError
                                , topic'RdKafkaMessageT     = ptrTopic
                                , partition'RdKafkaMessageT = producePartitionInt partition
                                , len'RdKafkaMessageT       = payloadLength
                                , payload'RdKafkaMessageT   = payloadPtr
                                , offset'RdKafkaMessageT    = 0
                                , keyLen'RdKafkaMessageT    = keyLength
                                , key'RdKafkaMessageT       = keyPtr
                                }

closeProducer :: Kafka -> IO ()
closeProducer _ = return ()

-- | Drains the outbound queue for a producer. This function is called automatically at the end of
-- 'runKafkaProducer' or 'runKafkaProducerConf' and usually doesn't need to be called directly.
drainOutQueue :: Kafka -> IO ()
drainOutQueue k = do
    pollEvents k 100
    l <- outboundQueueLength k
    unless (l == 0) $ drainOutQueue k
------------------------------------------------------------------------------------
keyAndPayload :: ProducerRecord -> (Maybe BS.ByteString, ProducePartition, BS.ByteString)
keyAndPayload (ProducerRecord partition payload) = (Nothing, partition, payload)
keyAndPayload (KeyedProducerRecord key partition payload) = (Just key, partition, payload)

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

pollEvents :: Kafka -> Int -> IO ()
pollEvents (Kafka kPtr _) timeout = void (rdKafkaPoll kPtr timeout)

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka kPtr _) = rdKafkaOutqLen kPtr
