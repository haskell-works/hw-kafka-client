module Kafka
( fetchBrokerMetadata
, withKafkaProducer
, produceMessageBatch
, getAllMetadata
, getTopicMetadata

-- Internal objects
, IS.newKafka
, IS.newKafkaTopic
, IS.dumpConfFromKafka
, IS.dumpConfFromKafkaTopic
, IS.setLogLevel
, IS.hPrintSupportedKafkaConf
, IS.hPrintKafka
, rdKafkaVersionStr

-- Type re-exports
, IT.Kafka
, IT.KafkaTopic

, IT.KafkaOffset(..)

, PIT.KafkaProduceMessage(..)
, PIT.KafkaProducePartition(..)

, IT.KafkaMetadata(..)
, IT.KafkaBrokerMetadata(..)
, IT.KafkaTopicMetadata(..)
, IT.KafkaPartitionMetadata(..)

, IT.KafkaLogLevel(..)
, IT.KafkaError(..)
, RDE.RdKafkaRespErrT

-- Pseudo-internal
, addBrokers
, drainOutQueue

) where

import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Types
import           Kafka.Producer
import           Kafka.Producer.Internal.Convert

import           Control.Exception
import           Control.Monad
import           Foreign                         hiding (void)
import           Foreign.C.String

import qualified Data.ByteString.Internal        as BSI
import qualified Kafka.Internal.RdKafkaEnum      as RDE
import qualified Kafka.Internal.Setup            as IS
import qualified Kafka.Internal.Types            as IT
import qualified Kafka.Producer.Internal.Types   as PIT

-- | Produce a batch of messages. Since librdkafka is backed by a queue, this function can return
-- before messages are sent. See 'drainOutQueue' to wait for the queue to be empty.
produceMessageBatch :: KafkaTopic  -- ^ topic pointer
                    -> KafkaProducePartition -- ^ partition to produce to. Specify 'KafkaUnassignedPartition' if you don't care, or you have keyed messsages.
                    -> [KafkaProduceMessage] -- ^ list of messages to enqueue.
                    -> IO [(KafkaProduceMessage, KafkaError)] -- list of failed messages with their errors. This will be empty on success.
produceMessageBatch (KafkaTopic topicPtr _ _) partition pms = do
  storables <- forM pms produceMessageToMessage
  withArray storables $ \batchPtr -> do
    batchPtrF <- newForeignPtr_ batchPtr
    numRet    <- rdKafkaProduceBatch topicPtr partitionInt copyMsgFlags batchPtrF (length storables)
    if numRet == length storables then return []
    else do
      errs <- mapM (return . err'RdKafkaMessageT <=< peekElemOff batchPtr)
                   [0..(fromIntegral $ length storables - 1)]
      return [(m, KafkaResponseError e) | (m, e) <- zip pms errs, e /= RdKafkaRespErrNoError]
  where
    partitionInt = producePartitionInteger partition
    produceMessageToMessage (KafkaProduceMessage bs) =  do
        let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr bs
        withForeignPtr topicPtr $ \ptrTopic ->
            withForeignPtr payloadFPtr $ \payloadPtr -> do
              let passedPayload = payloadPtr `plusPtr` payloadOffset
              return RdKafkaMessageT
                  { err'RdKafkaMessageT       = RdKafkaRespErrNoError
                  , topic'RdKafkaMessageT     = ptrTopic
                  , partition'RdKafkaMessageT = fromIntegral partitionInt
                  , len'RdKafkaMessageT       = payloadLength
                  , payload'RdKafkaMessageT   = passedPayload
                  , offset'RdKafkaMessageT    = 0
                  , keyLen'RdKafkaMessageT    = 0
                  , key'RdKafkaMessageT       = nullPtr
                  }
    produceMessageToMessage (KafkaProduceKeyedMessage kbs bs) =  do
        let (payloadFPtr, payloadOffset, payloadLength) = BSI.toForeignPtr bs
            (keyFPtr, keyOffset, keyLength) = BSI.toForeignPtr kbs

        withForeignPtr topicPtr $ \ptrTopic ->
            withForeignPtr payloadFPtr $ \payloadPtr ->
              withForeignPtr keyFPtr $ \keyPtr -> do
                let passedPayload = payloadPtr `plusPtr` payloadOffset
                    passedKey = keyPtr `plusPtr` keyOffset

                return RdKafkaMessageT
                   { err'RdKafkaMessageT       = RdKafkaRespErrNoError
                   , topic'RdKafkaMessageT     = ptrTopic
                   , partition'RdKafkaMessageT = fromIntegral partitionInt
                   , len'RdKafkaMessageT       = payloadLength
                   , payload'RdKafkaMessageT   = passedPayload
                   , offset'RdKafkaMessageT    = 0
                   , keyLen'RdKafkaMessageT    = keyLength
                   , key'RdKafkaMessageT       = passedKey
                   }

-- | Connects to Kafka broker in producer mode for a given topic, taking a function
-- that is fed with 'Kafka' and 'KafkaTopic' instances. After receiving handles you
-- should be using 'produceMessage', 'produceKeyedMessage' and 'produceMessageBatch'
-- to publish messages. This function drains the outbound queue automatically before returning.
withKafkaProducer :: ConfigOverrides -- ^ config overrides for kafka. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> ConfigOverrides -- ^ config overrides for topic. See <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>. Use an empty list if you don't care.
                  -> String -- ^ broker string, e.g. localhost:9092
                  -> String -- ^ topic name
                  -> (Kafka -> KafkaTopic -> IO a)  -- ^ your code, fed with 'Kafka' and 'KafkaTopic' instances for subsequent interaction.
                  -> IO a -- ^ returns what your code does
withKafkaProducer configOverrides topicConfigOverrides brokerString tName cb =
  bracket
    (do
      kafka <- newKafka RdKafkaProducer configOverrides
      addBrokers kafka brokerString
      topic <- newKafkaTopic kafka tName topicConfigOverrides
      return (kafka, topic)
    )
    (\(kafka, _) -> drainOutQueue kafka)
    (uncurry cb)

-- | Opens a connection with brokers and returns metadata about topics, partitions and brokers.
fetchBrokerMetadata :: ConfigOverrides -- ^ connection overrides, see <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
                    -> String  -- broker connection string, e.g. localhost:9092
                    -> Int -- timeout for the request, in milliseconds (10^3 per second)
                    -> IO (Either KafkaError KafkaMetadata) -- Left on error, Right with metadata on success
fetchBrokerMetadata configOverrides brokerString timeout = do
  kafka <- newKafka RdKafkaConsumer configOverrides
  addBrokers kafka brokerString
  getAllMetadata kafka timeout

-- | Grabs all metadata from a given Kafka instance.
getAllMetadata :: Kafka
               -> Int  -- ^ timeout in milliseconds (10^3 per second)
               -> IO (Either KafkaError KafkaMetadata)
getAllMetadata k = getMetadata k Nothing

-- | Grabs topic metadata from a given Kafka topic instance
getTopicMetadata :: Kafka
                 -> KafkaTopic
                 -> Int  -- ^ timeout in milliseconds (10^3 per second)
                 -> IO (Either KafkaError KafkaTopicMetadata)
getTopicMetadata k kt timeout = do
  err <- getMetadata k (Just kt) timeout
  case err of
    Left e -> return $ Left e
    Right md -> case topics md of
      [Left e]    -> return $ Left e
      [Right tmd] -> return $ Right tmd
      _ -> return $ Left $ KafkaError "Incorrect number of topics returned"

getMetadata :: Kafka -> Maybe KafkaTopic -> Int -> IO (Either KafkaError KafkaMetadata)
getMetadata (Kafka kPtr _) mTopic timeout = alloca $ \mdDblPtr -> do
    err <- case mTopic of
      Just (KafkaTopic kTopicPtr _ _) ->
        rdKafkaMetadata kPtr False kTopicPtr mdDblPtr timeout
      Nothing -> do
        nullTopic <- newForeignPtr_ nullPtr
        rdKafkaMetadata kPtr True nullTopic mdDblPtr timeout

    case err of
      RdKafkaRespErrNoError -> do
        mdPtr <- peek mdDblPtr
        md <- peek mdPtr
        retMd <- constructMetadata md
        rdKafkaMetadataDestroy mdPtr
        return $ Right retMd
      e -> return $ Left $ KafkaResponseError e

    where
      constructMetadata md =  do
        let nBrokers   = brokerCnt'RdKafkaMetadataT md
            brokersPtr = brokers'RdKafkaMetadataT md
            nTopics    = topicCnt'RdKafkaMetadataT md
            topicsPtr  = topics'RdKafkaMetadataT md

        brokerMds <- mapM (constructBrokerMetadata <=< peekElemOff brokersPtr) [0..(fromIntegral nBrokers - 1)]
        topicMds  <- mapM (constructTopicMetadata <=< peekElemOff topicsPtr)   [0..(fromIntegral nTopics - 1)]
        return $ KafkaMetadata brokerMds topicMds

      constructBrokerMetadata bmd = do
        hostStr <- peekCString (host'RdKafkaMetadataBrokerT bmd)
        return $ KafkaBrokerMetadata
                  (id'RdKafkaMetadataBrokerT bmd)
                  hostStr
                  (port'RdKafkaMetadataBrokerT bmd)

      constructTopicMetadata tmd =
        case err'RdKafkaMetadataTopicT tmd of
          RdKafkaRespErrNoError -> do
            let nPartitions   = partitionCnt'RdKafkaMetadataTopicT tmd
                partitionsPtr = partitions'RdKafkaMetadataTopicT tmd

            topicStr <- peekCString (topic'RdKafkaMetadataTopicT tmd)
            partitionsMds <- mapM (constructPartitionMetadata <=< peekElemOff partitionsPtr) [0..(fromIntegral nPartitions - 1)]
            return $ Right $ KafkaTopicMetadata topicStr partitionsMds
          e -> return $ Left $ KafkaResponseError e

      constructPartitionMetadata pmd =
        case err'RdKafkaMetadataPartitionT pmd of
          RdKafkaRespErrNoError -> do
            let nReplicas   = replicaCnt'RdKafkaMetadataPartitionT pmd
                replicasPtr = replicas'RdKafkaMetadataPartitionT pmd
                nIsrs       = isrCnt'RdKafkaMetadataPartitionT pmd
                isrsPtr     = isrs'RdKafkaMetadataPartitionT pmd
            replicas <- mapM (peekElemOff replicasPtr) [0..(fromIntegral nReplicas - 1)]
            isrs     <- mapM (peekElemOff isrsPtr) [0..(fromIntegral nIsrs - 1)]
            return $ Right $ KafkaPartitionMetadata
              (id'RdKafkaMetadataPartitionT pmd)
              (leader'RdKafkaMetadataPartitionT pmd)
              (map fromIntegral replicas)
              (map fromIntegral isrs)
          e -> return $ Left $ KafkaResponseError e




