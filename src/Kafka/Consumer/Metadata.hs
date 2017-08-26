module Kafka.Consumer.Metadata
( KafkaMetadata(..), BrokerMetadata(..), TopicMetadata(..), PartitionMetadata(..)
, WatermarkOffsets(..)
, GroupMemberId(..), GroupMemberInfo(..)
, GroupProtocolType(..), GroupProtocol(..), GroupState(..)
, GroupInfo(..)
, allTopicsMetadata, topicMetadata
, watermarkOffsets, watermarkOffsets'
, partitionWatermarkOffsets
, allConsumerGroupsInfo, consumerGroupInfo
)
where

import Control.Arrow          (left)
import Control.Exception      (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Bifunctor
import Data.ByteString        (ByteString, pack)
import Data.Semigroup         ((<>))
import Foreign
import Foreign.C.String
import Kafka.Consumer.Types
import Kafka.Internal.RdKafka
import Kafka.Internal.Shared
import Kafka.Types

data KafkaMetadata = KafkaMetadata
  { kmBrokers    :: [BrokerMetadata]
  , kmTopics     :: [TopicMetadata]
  , kmOrigBroker :: !BrokerId
  } deriving (Show, Eq)

data BrokerMetadata = BrokerMetadata
  { bmBrokerId   :: !BrokerId
  , bmBrokerHost :: !String
  , bmBrokerPort :: !Int
  } deriving (Show, Eq)

data PartitionMetadata = PartitionMetadata
  { pmPartitionId    :: !PartitionId
  , pmError          :: Maybe KafkaError
  , pmLeader         :: !BrokerId
  , pmReplicas       :: [PartitionId]
  , pmInSyncReplicas :: [PartitionId]
  } deriving (Show, Eq)

data TopicMetadata = TopicMetadata
  { tmTopicName  :: !TopicName
  , tmPartitions :: [PartitionMetadata]
  , tmError      :: Maybe KafkaError
  } deriving (Show, Eq)

data WatermarkOffsets = WatermarkOffsets
  { woTopicName     :: !TopicName
  , woPartitionId   :: !PartitionId
  , woLowWatermark  :: !Offset
  , woHighWatermark :: !Offset
  } deriving (Show, Eq)

newtype GroupMemberId = GroupMemberId String deriving (Show, Eq, Read, Ord)
data GroupMemberInfo = GroupMemberInfo
  { gmiMemberId   :: !GroupMemberId
  , gmiClientId   :: !ClientId
  , gmiClientHost :: !String
  , gmiMetadata   :: !ByteString
  , gmiAssignment :: !ByteString
  } deriving (Show, Eq)

newtype GroupProtocolType = GroupProtocolType String deriving (Show, Eq, Read, Ord)
newtype GroupProtocol = GroupProtocol String  deriving (Show, Eq, Read, Ord)
data GroupState
  = GroupPreparingRebalance       -- ^ Group is preparing to rebalance
  | GroupEmpty                    -- ^ Group has no more members, but lingers until all offsets have expired
  | GroupAwaitingSync             -- ^ Group is awaiting state assignment from the leader
  | GroupStable                   -- ^ Group is stable
  | GroupDead                     -- ^ Group has no more members and its metadata is being removed
  deriving (Show, Eq, Read, Ord)

data GroupInfo = GroupInfo
  { giGroup        :: !ConsumerGroupId
  , giError        :: Maybe KafkaError
  , giState        :: !GroupState
  , giProtocolType :: !GroupProtocolType
  , giProtocol     :: !GroupProtocol
  , giMembers      :: [GroupMemberInfo]
  } deriving (Show, Eq)

-- | Returns metadata for all topics in the cluster
allTopicsMetadata :: MonadIO m => KafkaConsumer -> m (Either KafkaError KafkaMetadata)
allTopicsMetadata (KafkaConsumer k _) = liftIO $ do
  meta <- rdKafkaMetadata k True Nothing
  traverse fromKafkaMetadataPtr (left KafkaResponseError meta)

-- | Returns metadata only for specified topic
topicMetadata :: MonadIO m => KafkaConsumer -> TopicName -> m (Either KafkaError KafkaMetadata)
topicMetadata (KafkaConsumer k _) (TopicName tn) = liftIO $
  bracket mkTopic clTopic $ \mbt -> case mbt of
    Left err -> return (Left $ KafkaError err)
    Right t -> do
      meta <- rdKafkaMetadata k False (Just t)
      traverse fromKafkaMetadataPtr (left KafkaResponseError meta)
  where
    mkTopic = newRdKafkaTopicConfT >>= newUnmanagedRdKafkaTopicT k tn
    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets :: MonadIO m => KafkaConsumer -> TopicName -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets k t = do
  meta <- topicMetadata k t
  case meta of
    Left err -> return [Left err]
    Right tm -> if null (kmTopics tm)
                  then return []
                  else watermarkOffsets' k (head $ kmTopics tm)

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets' :: MonadIO m => KafkaConsumer -> TopicMetadata -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets' k tm =
  let pids = pmPartitionId <$> tmPartitions tm
  in liftIO $ traverse (partitionWatermarkOffsets k (tmTopicName tm)) pids

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a specific partition
partitionWatermarkOffsets :: MonadIO m => KafkaConsumer -> TopicName -> PartitionId -> m (Either KafkaError WatermarkOffsets)
partitionWatermarkOffsets (KafkaConsumer k _) (TopicName t) (PartitionId p) = liftIO $ do
  offs <- rdKafkaQueryWatermarkOffsets k t p 0
  return $ bimap KafkaResponseError toWatermark offs
  where
    toWatermark (l, h) = WatermarkOffsets (TopicName t) (PartitionId p) (Offset l) (Offset h)

allConsumerGroupsInfo :: MonadIO m => KafkaConsumer -> m (Either KafkaError [GroupInfo])
allConsumerGroupsInfo (KafkaConsumer k _) = liftIO $ do
  res <- rdKafkaListGroups k Nothing (-1)
  traverse fromGroupInfoListPtr (left KafkaResponseError res)

consumerGroupInfo :: MonadIO m => KafkaConsumer -> ConsumerGroupId -> m (Either KafkaError [GroupInfo])
consumerGroupInfo (KafkaConsumer k _) (ConsumerGroupId gn) = liftIO $ do
  res <- rdKafkaListGroups k (Just gn) (-1)
  traverse fromGroupInfoListPtr (left KafkaResponseError res)

-------------------------------------------------------------------------------
fromGroupInfoListPtr :: RdKafkaGroupListTPtr -> IO [GroupInfo]
fromGroupInfoListPtr ptr =
  withForeignPtr ptr $ \realPtr -> do
    gl   <- peek realPtr
    pgis <- peekArray (groupCnt'RdKafkaGroupListT gl) (groups'RdKafkaGroupListT gl)
    traverse fromGroupInfoPtr pgis

fromGroupInfoPtr :: RdKafkaGroupInfoT -> IO GroupInfo
fromGroupInfoPtr gi = do
  --bmd <- peek (broker'RdKafkaGroupInfoT gi) -- >>= fromBrokerMetadataPtr
  --xxx <- fromBrokerMetadataPtr bmd
  cid <- peekCAString $ group'RdKafkaGroupInfoT gi
  stt <- peekCAString $ state'RdKafkaGroupInfoT gi
  prt <- peekCAString $ protocolType'RdKafkaGroupInfoT gi
  pr  <- peekCAString $ protocol'RdKafkaGroupInfoT gi
  mbs <- peekArray (memberCnt'RdKafkaGroupInfoT gi) (members'RdKafkaGroupInfoT gi)
  mbl <- mapM fromGroupMemberInfoPtr mbs
  return GroupInfo
    { --giBroker = bmd
     giGroup          = ConsumerGroupId cid
    , giError         = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaGroupInfoT gi)
    , giState         = groupStateFromKafkaString stt
    , giProtocolType  = GroupProtocolType prt
    , giProtocol      = GroupProtocol pr
    , giMembers       = mbl
    }

fromGroupMemberInfoPtr :: RdKafkaGroupMemberInfoT -> IO GroupMemberInfo
fromGroupMemberInfoPtr mi = do
  mid <- peekCAString $ memberId'RdKafkaGroupMemberInfoT mi
  cid <- peekCAString $ clientId'RdKafkaGroupMemberInfoT mi
  hst <- peekCAString $ clientHost'RdKafkaGroupMemberInfoT mi
  mtd <- peekArray (memberMetadataSize'RdKafkaGroupMemberInfoT mi) (memberMetadata'RdKafkaGroupMemberInfoT mi)
  ass <- peekArray (memberAssignmentSize'RdKafkaGroupMemberInfoT mi) (memberAssignment'RdKafkaGroupMemberInfoT mi)
  return GroupMemberInfo
    { gmiMemberId   = GroupMemberId mid
    , gmiClientId   = ClientId cid
    , gmiClientHost = hst
    , gmiMetadata   = pack mtd
    , gmiAssignment = pack ass
    }

fromTopicMetadataPtr :: RdKafkaMetadataTopicT -> IO TopicMetadata
fromTopicMetadataPtr tm = do
  tnm <- peekCAString (topic'RdKafkaMetadataTopicT tm)
  pts <- peekArray (partitionCnt'RdKafkaMetadataTopicT tm) (partitions'RdKafkaMetadataTopicT tm)
  pms <- mapM fromPartitionMetadataPtr pts
  return TopicMetadata
    { tmTopicName   = TopicName tnm
    , tmPartitions  = pms
    , tmError       = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataTopicT tm)
    }

fromPartitionMetadataPtr :: RdKafkaMetadataPartitionT -> IO PartitionMetadata
fromPartitionMetadataPtr pm = do
  reps <- peekArray (replicaCnt'RdKafkaMetadataPartitionT pm) (replicas'RdKafkaMetadataPartitionT pm)
  isrs <- peekArray (isrCnt'RdKafkaMetadataPartitionT pm) (isrs'RdKafkaMetadataPartitionT pm)
  return PartitionMetadata
    { pmPartitionId     = PartitionId (id'RdKafkaMetadataPartitionT pm)
    , pmError           = kafkaErrorToMaybe $ KafkaResponseError (err'RdKafkaMetadataPartitionT pm)
    , pmLeader          = BrokerId (leader'RdKafkaMetadataPartitionT pm)
    , pmReplicas        = (PartitionId . fromIntegral) <$> reps
    , pmInSyncReplicas  = (PartitionId . fromIntegral) <$> isrs
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

groupStateFromKafkaString :: String -> GroupState
groupStateFromKafkaString s = case s of
  "PreparingRebalance" -> GroupPreparingRebalance
  "AwaitingSync"       -> GroupAwaitingSync
  "Stable"             -> GroupStable
  "Dead"               -> GroupDead
  "Empty"              -> GroupEmpty
  _                    -> error $ "Unknown group state: " <> s
