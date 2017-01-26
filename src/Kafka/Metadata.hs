module Kafka.Metadata
( getBrokerMetadata
, getAllMetadata
, getTopicMetadata
, MIT.KafkaMetadata (..)
, MIT.BrokerMetadata (..)
, MIT.TopicMetadata (..)
, MIT.PartitionMetadata (..)
)

where

import           Foreign
import           Foreign.C.String

import           Control.Monad

import           Kafka
import           Kafka.Consumer
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Metadata.Types

import qualified Kafka.Metadata.Types as MIT

-- | Opens a connection with brokers and returns metadata about topics, partitions and brokers.
getBrokerMetadata :: KafkaProps       -- ^ connection overrides, see <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
                  -> [BrokerAddress]  -- ^ broker connection string, e.g. localhost:9092
                  -> Timeout          -- ^ timeout for the request, in milliseconds (10^3 per second)
                  -> IO (Either KafkaError KafkaMetadata) -- Left on error, Right with metadata on success
getBrokerMetadata c bs t = do
  conf  <- kafkaConf c
  kafka <- newConsumer bs conf
  getAllMetadata kafka t

-- | Grabs all metadata from a given Kafka instance.
getAllMetadata :: Kafka
               -> Timeout  -- ^ timeout in milliseconds (10^3 per second)
               -> IO (Either KafkaError KafkaMetadata)
getAllMetadata k = getMetadata k Nothing

-- | Grabs topic metadata from a given Kafka topic instance
getTopicMetadata :: Kafka
                 -> KafkaTopic
                 -> Timeout  -- ^ timeout in milliseconds (10^3 per second)
                 -> IO (Either KafkaError TopicMetadata)
getTopicMetadata k kt t = do
  err <- getMetadata k (Just kt) t
  case err of
    Left e -> return $ Left e
    Right md -> case topics md of
      [Left e]    -> return $ Left e
      [Right tmd] -> return $ Right tmd
      _ -> return . Left $ KafkaError "Incorrect number of topics returned"

getMetadata :: Kafka -> Maybe KafkaTopic -> Timeout -> IO (Either KafkaError KafkaMetadata)
getMetadata (Kafka kPtr _) mTopic (Timeout ms) = alloca $ \mdDblPtr -> do
    err <- case mTopic of
      Just (KafkaTopic kTopicPtr _ _) ->
        rdKafkaMetadata kPtr False kTopicPtr mdDblPtr ms
      Nothing -> do
        nullTopic <- newForeignPtr_ nullPtr
        rdKafkaMetadata kPtr True nullTopic mdDblPtr ms

    case err of
      RdKafkaRespErrNoError -> do
        mdPtr <- peek mdDblPtr
        md <- peek mdPtr
        retMd <- constructMetadata md
        rdKafkaMetadataDestroy mdPtr
        return $ Right retMd
      e -> return . Left $ KafkaResponseError e

    where
      constructMetadata md =  do
        let nBrokers   = brokerCnt'RdKafkaMetadataT md
            brokersPtr = brokers'RdKafkaMetadataT md
            nTopics    = topicCnt'RdKafkaMetadataT md
            topicsPtr  = topics'RdKafkaMetadataT md

        brokerMds <- mapM (constructBrokerMetadata <=< peekElemOff brokersPtr) [0..(fromIntegral nBrokers - 1)]
        topicMds  <- mapM (constructTopicMetadata  <=< peekElemOff topicsPtr)  [0..(fromIntegral nTopics - 1)]
        return $ KafkaMetadata brokerMds topicMds

      constructBrokerMetadata bmd = do
        hostStr <- peekCString (host'RdKafkaMetadataBrokerT bmd)
        return $ BrokerMetadata
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
            return . Right $ TopicMetadata topicStr partitionsMds
          e -> return . Left $ KafkaResponseError e

      constructPartitionMetadata pmd =
        case err'RdKafkaMetadataPartitionT pmd of
          RdKafkaRespErrNoError -> do
            let nReplicas   = replicaCnt'RdKafkaMetadataPartitionT pmd
                replicasPtr = replicas'RdKafkaMetadataPartitionT pmd
                nIsrs       = isrCnt'RdKafkaMetadataPartitionT pmd
                isrsPtr     = isrs'RdKafkaMetadataPartitionT pmd
            replicas <- mapM (peekElemOff replicasPtr) [0..(fromIntegral nReplicas - 1)]
            isrs     <- mapM (peekElemOff isrsPtr) [0..(fromIntegral nIsrs - 1)]
            return . Right $ PartitionMetadata
              (id'RdKafkaMetadataPartitionT pmd)
              (leader'RdKafkaMetadataPartitionT pmd)
              (map fromIntegral replicas)
              (map fromIntegral isrs)
          e -> return . Left $ KafkaResponseError e
