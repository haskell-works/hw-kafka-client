module Kafka.Producer
( runProducerConf
, runProducer
, newKafkaProducerConf
, newKafkaProducer
, produceMessage
, drainOutQueue
, IS.newKafkaTopic
, IT.BrokersString (..)
, IT.Kafka
, IT.KafkaError (..)
, PIT.KafkaProduceMessage (..)
, PIT.KafkaProducePartition (..)
, RDE.RdKafkaRespErrT (..)
)
where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Internal        as BSI
import           Foreign                         hiding (void)
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Types
import           Kafka.Producer.Internal.Convert
import           Kafka.Producer.Internal.Types

import qualified Kafka.Internal.RdKafkaEnum      as RDE
import qualified Kafka.Internal.Setup            as IS
import qualified Kafka.Internal.Types            as IT
import qualified Kafka.Producer.Internal.Types   as PIT

runProducerConf :: KafkaConf
                -> BrokersString
                -> (Kafka -> IO a)
                -> IO a
runProducerConf c bs f =
    bracket mkProducer clProducer runHandler
    where
        mkProducer = newKafkaProducer bs c
        clProducer = drainOutQueue
        runHandler = f

runProducer :: ConfigOverrides     -- ^ Extra kafka consumer parameters (see kafka documentation)
            -> BrokersString       -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
            -> (Kafka -> IO a)
            -> IO a
runProducer c bs f = do
    conf <- newKafkaProducerConf c
    runProducerConf conf bs f

-- | Creates a new kafka configuration for a producer'.
newKafkaProducerConf :: ConfigOverrides  -- ^ Extra kafka producer parameters (see kafka documentation)
                     -> IO KafkaConf     -- ^ Kafka configuration which can be altered before it is used in 'newKafkaProducer'
newKafkaProducerConf =
    kafkaConf

-- | Creates a new kafka producer
newKafkaProducer :: BrokersString -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
                 -> KafkaConf     -- ^ Kafka configuration for a producer (see 'newKafkaProducerConf')
                 -> IO Kafka      -- ^ Kafka instance
newKafkaProducer (BrokersString bs) conf = do
    kafka <- newKafkaPtr RdKafkaProducer conf
    addBrokers kafka bs
    return kafka

-- | Produce a single unkeyed message to either a random partition or specified partition. Since
-- librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'drainOutQueue' to wait for queue to empty.
produceMessage :: KafkaTopic             -- ^ target topic
               -> KafkaProducePartition  -- ^ the "default" partition to produce to. Only used for messages where with no message key specified.
               -> KafkaProduceMessage    -- ^ the message to enqueue. This function is undefined for keyed messages.
               -> IO (Maybe KafkaError)  -- ^ 'Nothing' on success, error if something went wrong.
produceMessage (KafkaTopic topicPtr _ _) partition message =
    let (key, payload) = keyAndPayload message
    in  withBS (Just payload) $ \payloadPtr payloadLength ->
            withBS key $ \keyPtr keyLength ->
                let realPart = if keyLength == 0 then partition else KafkaUnassignedPartition
                in  handleProduceErr =<<
                        rdKafkaProduce topicPtr (producePartitionInteger realPart)
                          copyMsgFlags payloadPtr (fromIntegral payloadLength)
                          keyPtr (fromIntegral keyLength) nullPtr
    where
        keyAndPayload (KafkaProduceMessage payload) = (Nothing, payload)
        keyAndPayload (KafkaProduceKeyedMessage key payload) = (Just key, payload)

------------------------------------------------------------------------------------
withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

pollEvents :: Kafka -> Int -> IO ()
pollEvents (Kafka kPtr _) timeout = void (rdKafkaPoll kPtr timeout)

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka kPtr _) = rdKafkaOutqLen kPtr

-- | Drains the outbound queue for a producer. This function is called automatically at the end of
-- 'runKafkaProducer' or 'runKafkaProducerConf' and usually doesn't need to be called directly.
drainOutQueue :: Kafka -> IO ()
drainOutQueue k = do
    pollEvents k 100
    l <- outboundQueueLength k
    unless (l == 0) $ drainOutQueue k
