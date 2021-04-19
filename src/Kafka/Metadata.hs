{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

-----------------------------------------------------------------------------
-- |
-- Module with metadata types and functions.
-----------------------------------------------------------------------------
module Kafka.Metadata
( KafkaMetadata(..), BrokerMetadata(..), TopicMetadata(..), PartitionMetadata(..)
, WatermarkOffsets(..)
, GroupMemberId(..), GroupMemberInfo(..)
, GroupProtocolType(..), GroupProtocol(..), GroupState(..)
, GroupInfo(..)
, allTopicsMetadata, topicMetadata
, watermarkOffsets, watermarkOffsets'
, partitionWatermarkOffsets
, offsetsForTime, offsetsForTime', topicOffsetsForTime
, allConsumerGroupsInfo, consumerGroupInfo
)
where

import Control.Arrow          (left)
import Control.Exception      (bracket)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bifunctor         (bimap)
import Data.ByteString        (ByteString, pack)
import Data.Text              (Text)
import Foreign                (Storable (peek), peekArray, withForeignPtr)
import GHC.Generics           (Generic)
import Kafka.Consumer.Convert (fromNativeTopicPartitionList'', toNativeTopicPartitionList)
import Kafka.Consumer.Types   (ConsumerGroupId (..), Offset (..), PartitionOffset (..), TopicPartition (..))
import Kafka.Internal.RdKafka (RdKafkaGroupInfoT (..), RdKafkaGroupListT (..), RdKafkaGroupListTPtr, RdKafkaGroupMemberInfoT (..), RdKafkaMetadataBrokerT (..), RdKafkaMetadataPartitionT (..), RdKafkaMetadataT (..), RdKafkaMetadataTPtr, RdKafkaMetadataTopicT (..), RdKafkaRespErrT (..), RdKafkaTPtr, destroyUnmanagedRdKafkaTopic, newUnmanagedRdKafkaTopicT, peekCAText, rdKafkaListGroups, rdKafkaMetadata, rdKafkaOffsetsForTimes, rdKafkaQueryWatermarkOffsets)
import Kafka.Internal.Setup   (HasKafka (..), Kafka (..))
import Kafka.Internal.Shared  (kafkaErrorToMaybe)
import Kafka.Types            (BrokerId (..), ClientId (..), KafkaError (..), Millis (..), PartitionId (..), Timeout (..), TopicName (..))

import qualified Data.Set  as S
import qualified Data.Text as Text

data KafkaMetadata = KafkaMetadata
  { kmBrokers    :: [BrokerMetadata]
  , kmTopics     :: [TopicMetadata]
  , kmOrigBroker :: !BrokerId
  } deriving (Show, Eq, Generic)

data BrokerMetadata = BrokerMetadata
  { bmBrokerId   :: !BrokerId
  , bmBrokerHost :: !Text
  , bmBrokerPort :: !Int
  } deriving (Show, Eq, Generic)

data PartitionMetadata = PartitionMetadata
  { pmPartitionId    :: !PartitionId
  , pmError          :: Maybe KafkaError
  , pmLeader         :: !BrokerId
  , pmReplicas       :: [BrokerId]
  , pmInSyncReplicas :: [BrokerId]
  } deriving (Show, Eq, Generic)

data TopicMetadata = TopicMetadata
  { tmTopicName  :: !TopicName
  , tmPartitions :: [PartitionMetadata]
  , tmError      :: Maybe KafkaError
  } deriving (Show, Eq, Generic)

data WatermarkOffsets = WatermarkOffsets
  { woTopicName     :: !TopicName
  , woPartitionId   :: !PartitionId
  , woLowWatermark  :: !Offset
  , woHighWatermark :: !Offset
  } deriving (Show, Eq, Generic)

newtype GroupMemberId = GroupMemberId Text deriving (Show, Eq, Read, Ord)
data GroupMemberInfo = GroupMemberInfo
  { gmiMemberId   :: !GroupMemberId
  , gmiClientId   :: !ClientId
  , gmiClientHost :: !Text
  , gmiMetadata   :: !ByteString
  , gmiAssignment :: !ByteString
  } deriving (Show, Eq, Generic)

newtype GroupProtocolType = GroupProtocolType Text deriving (Show, Eq, Read, Ord, Generic)
newtype GroupProtocol = GroupProtocol Text  deriving (Show, Eq, Read, Ord, Generic)
data GroupState
  = GroupPreparingRebalance       -- ^ Group is preparing to rebalance
  | GroupEmpty                    -- ^ Group has no more members, but lingers until all offsets have expired
  | GroupAwaitingSync             -- ^ Group is awaiting state assignment from the leader
  | GroupStable                   -- ^ Group is stable
  | GroupDead                     -- ^ Group has no more members and its metadata is being removed
  deriving (Show, Eq, Read, Ord, Generic)

data GroupInfo = GroupInfo
  { giGroup        :: !ConsumerGroupId
  , giError        :: Maybe KafkaError
  , giState        :: !GroupState
  , giProtocolType :: !GroupProtocolType
  , giProtocol     :: !GroupProtocol
  , giMembers      :: [GroupMemberInfo]
  } deriving (Show, Eq, Generic)

-- | Returns metadata for all topics in the cluster
allTopicsMetadata :: (MonadIO m, HasKafka k) => k -> Timeout -> m (Either KafkaError KafkaMetadata)
allTopicsMetadata k (Timeout timeout) = liftIO $ do
  meta <- rdKafkaMetadata (getKafkaPtr k) True Nothing timeout
  traverse fromKafkaMetadataPtr (left KafkaResponseError meta)

-- | Returns metadata only for specified topic
topicMetadata :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> m (Either KafkaError KafkaMetadata)
topicMetadata k (Timeout timeout) (TopicName tn) = liftIO $
  bracket mkTopic clTopic $ \mbt -> case mbt of
    Left err -> return (Left $ KafkaError (Text.pack err))
    Right t -> do
      meta <- rdKafkaMetadata (getKafkaPtr k) False (Just t) timeout
      traverse fromKafkaMetadataPtr (left KafkaResponseError meta)
  where
    mkTopic = newUnmanagedRdKafkaTopicT (getKafkaPtr k) (Text.unpack tn) Nothing
    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets k timeout t = do
  meta <- topicMetadata k timeout t
  case meta of
    Left err -> return [Left err]
    Right tm -> if null (kmTopics tm)
                  then return []
                  else watermarkOffsets' k timeout (head $ kmTopics tm)

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a given topic.
watermarkOffsets' :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicMetadata -> m [Either KafkaError WatermarkOffsets]
watermarkOffsets' k timeout tm =
  let pids = pmPartitionId <$> tmPartitions tm
  in liftIO $ traverse (partitionWatermarkOffsets k timeout (tmTopicName tm)) pids

-- | Query broker for low (oldest/beginning) and high (newest/end) offsets for a specific partition
partitionWatermarkOffsets :: (MonadIO m, HasKafka k) => k -> Timeout -> TopicName -> PartitionId -> m (Either KafkaError WatermarkOffsets)
partitionWatermarkOffsets k (Timeout timeout) (TopicName t) (PartitionId p) = liftIO $ do
  offs <- rdKafkaQueryWatermarkOffsets (getKafkaPtr k) (Text.unpack t) p timeout
  return $ bimap KafkaResponseError toWatermark offs
  where
    toWatermark (l, h) = WatermarkOffsets (TopicName t) (PartitionId p) (Offset l) (Offset h)

-- | Look up the offsets for the given topic by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
topicOffsetsForTime :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> TopicName -> m (Either KafkaError [TopicPartition])
topicOffsetsForTime k timeout timestamp topic  = do
  meta <- topicMetadata k timeout topic
  case meta of
    Left err -> return $ Left err
    Right meta' ->
      let tps = [(tmTopicName t, pmPartitionId p)| t <- kmTopics meta', p <- tmPartitions t]
      in offsetsForTime k timeout timestamp tps

-- | Look up the offsets for the given metadata by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
offsetsForTime' :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> TopicMetadata -> m (Either KafkaError [TopicPartition])
offsetsForTime' k timeout timestamp t =
    let tps = [(tmTopicName t, pmPartitionId p) | p <- tmPartitions t]
    in offsetsForTime k timeout timestamp tps

-- | Look up the offsets for the given partitions by timestamp.
--
-- The returned offset for each partition is the earliest offset whose
-- timestamp is greater than or equal to the given timestamp in the
-- corresponding partition.
offsetsForTime :: (MonadIO m, HasKafka k) => k -> Timeout -> Millis -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
offsetsForTime k (Timeout timeout) (Millis t) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList $ mkTopicPartition <$> uniqueTps
  res <- rdKafkaOffsetsForTimes (getKafkaPtr k) ntps timeout
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err                   -> return $ Left (KafkaResponseError err)
  where
    uniqueTps = S.toList . S.fromList $ tps
    -- rd_kafka_offsets_for_times reuses `offset` to specify timestamp :(
    mkTopicPartition (tn, p) = TopicPartition tn p (PartitionOffset t)

-- | List and describe all consumer groups in cluster.
allConsumerGroupsInfo :: (MonadIO m, HasKafka k) => k -> Timeout -> m (Either KafkaError [GroupInfo])
allConsumerGroupsInfo k (Timeout t) = liftIO $ do
  res <- rdKafkaListGroups (getKafkaPtr k) Nothing t
  traverse fromGroupInfoListPtr (left KafkaResponseError res)

-- | Describe a given consumer group.
consumerGroupInfo :: (MonadIO m, HasKafka k) => k -> Timeout -> ConsumerGroupId -> m (Either KafkaError [GroupInfo])
consumerGroupInfo k (Timeout timeout) (ConsumerGroupId gn) = liftIO $ do
  res <- rdKafkaListGroups (getKafkaPtr k) (Just (Text.unpack gn)) timeout
  traverse fromGroupInfoListPtr (left KafkaResponseError res)

-------------------------------------------------------------------------------
getKafkaPtr :: HasKafka k => k -> RdKafkaTPtr
getKafkaPtr k = let (Kafka k') = getKafka k in k'
{-# INLINE getKafkaPtr #-}


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
  cid <- peekCAText $ group'RdKafkaGroupInfoT gi
  stt <- peekCAText $ state'RdKafkaGroupInfoT gi
  prt <- peekCAText $ protocolType'RdKafkaGroupInfoT gi
  pr  <- peekCAText $ protocol'RdKafkaGroupInfoT gi
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
  mid <- peekCAText $ memberId'RdKafkaGroupMemberInfoT mi
  cid <- peekCAText $ clientId'RdKafkaGroupMemberInfoT mi
  hst <- peekCAText $ clientHost'RdKafkaGroupMemberInfoT mi
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
  tnm <- peekCAText (topic'RdKafkaMetadataTopicT tm)
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
    , pmReplicas        = BrokerId . fromIntegral <$> reps
    , pmInSyncReplicas  = BrokerId . fromIntegral <$> isrs
    }


fromBrokerMetadataPtr :: RdKafkaMetadataBrokerT -> IO BrokerMetadata
fromBrokerMetadataPtr bm = do
    host <- peekCAText (host'RdKafkaMetadataBrokerT bm)
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

groupStateFromKafkaString :: Text -> GroupState
groupStateFromKafkaString s = case s of
  "PreparingRebalance" -> GroupPreparingRebalance
  "AwaitingSync"       -> GroupAwaitingSync
  "Stable"             -> GroupStable
  "Dead"               -> GroupDead
  "Empty"              -> GroupEmpty
  _                    -> error $ "Unknown group state: " <> Text.unpack s
