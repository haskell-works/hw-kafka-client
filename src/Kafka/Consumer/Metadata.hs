module Kafka.Consumer.Metadata
( KafkaMetadata(..), BrokerMetadata(..), TopicMetadata(..), PartitionMetadata(..)
, allTopicsMetadata
, topicMetadata
)
where

import Control.Arrow          (left)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Foreign
import Foreign.C.String
import Kafka.Consumer.Types
import Kafka.Internal.RdKafka
import Kafka.Internal.Shared
import Kafka.Types

data KafkaMetadata = KafkaMetadata
  { kmBrokers    :: [BrokerMetadata]
  , kmTopics     :: [TopicMetadata]
  , kmOrigBroker :: BrokerId
  } deriving (Show, Eq)

data BrokerMetadata = BrokerMetadata
  { bmBrokerId   :: BrokerId
  , bmBrokerHost :: String
  , bmBrokerPort :: Int
  } deriving (Show, Eq)

data PartitionMetadata = PartitionMetadata
  { pmPartitionId    :: PartitionId
  , pmError          :: Maybe KafkaError
  , pmLeader         :: BrokerId
  , pmReplicas       :: [PartitionId]
  , pmInSyncReplicas :: [PartitionId]
  } deriving (Show, Eq)

data TopicMetadata = TopicMetadata
  { tmTopicName  :: TopicName
  , tmPartitions :: [PartitionMetadata]
  , tmError      :: Maybe KafkaError
  } deriving (Show, Eq)

-- | Returns metadata for all topics in the cluster
allTopicsMetadata :: MonadIO m => KafkaConsumer -> m (Either KafkaError KafkaMetadata)
allTopicsMetadata (KafkaConsumer k _) = liftIO $ do
  meta <- rdKafkaMetadata k True Nothing
  traverse fromKafkaMetadataPtr (left KafkaResponseError meta)

-- | Returns metadata only for specified topic
topicMetadata :: MonadIO m => KafkaConsumer -> TopicName -> m (Either KafkaError KafkaMetadata)
topicMetadata (KafkaConsumer k _) (TopicName tn) = liftIO $ do
  tc <- newRdKafkaTopicConfT
  mbt <- newRdKafkaTopicT k tn tc
  case mbt of
    Left err -> return (Left $ KafkaError err)
    Right t -> do
      meta <- rdKafkaMetadata k False (Just t)
      traverse fromKafkaMetadataPtr (left KafkaResponseError meta)

-------------------------------------------------------------------------------

fromTopicMetadataPtr :: RdKafkaMetadataTopicT -> IO TopicMetadata
fromTopicMetadataPtr tm = do
  tnm <- peekCAString (topic'RdKafkaMetadataTopicT tm)
  pts <- peekArray (partitionCnt'RdKafkaMetadataTopicT tm) (partitions'RdKafkaMetadataTopicT tm)
  pms <- mapM fromPartitionMetadataPtr pts
  return TopicMetadata
    { tmTopicName = TopicName tnm
    , tmPartitions = pms
    , tmError = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataTopicT tm)
    }

fromPartitionMetadataPtr :: RdKafkaMetadataPartitionT -> IO PartitionMetadata
fromPartitionMetadataPtr pm = do
  reps <- peekArray (replicaCnt'RdKafkaMetadataPartitionT pm) (replicas'RdKafkaMetadataPartitionT pm)
  isrs <- peekArray (isrCnt'RdKafkaMetadataPartitionT pm) (isrs'RdKafkaMetadataPartitionT pm)
  return PartitionMetadata
    { pmPartitionId = PartitionId (id'RdKafkaMetadataPartitionT pm)
    , pmError = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataPartitionT pm)
    , pmLeader = BrokerId (leader'RdKafkaMetadataPartitionT pm)
    , pmReplicas = (PartitionId . fromIntegral) <$> reps
    , pmInSyncReplicas = (PartitionId . fromIntegral) <$> isrs
    }


fromBrokerMetadataPtr :: RdKafkaMetadataBrokerT -> IO BrokerMetadata
fromBrokerMetadataPtr bm = do
    host <- peekCAString (host'RdKafkaMetadataBrokerT bm)
    return BrokerMetadata
      { bmBrokerId   = BrokerId (id'RdKafkaMetadataBrokerT bm)
      , bmBrokerHost = host
      , bmBrokerPort = port'RdKafkaMetadataBrokerT bm
      }


fromKafkaMetadataPtr :: RdKafkaMetadataTPtr -> IO KafkaMetadata
fromKafkaMetadataPtr ptr =
  withForeignPtr ptr $ \realPtr -> do
    km    <- peek realPtr
    pbms  <- peekArray (brokerCnt'RdKafkaMetadataT km) (brokers'RdKafkaMetadataT km)
    bms   <- mapM fromBrokerMetadataPtr pbms
    ptms  <- peekArray (topicCnt'RdKafkaMetadataT km) (topics'RdKafkaMetadataT km)
    tms   <- mapM fromTopicMetadataPtr ptms
    return KafkaMetadata
      { kmBrokers = bms
      , kmTopics  = tms
      , kmOrigBroker = BrokerId $ fromIntegral (origBrokerId'RdKafkaMetadataT km)
      }

