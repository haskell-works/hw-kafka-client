{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module holding consumer types.
-----------------------------------------------------------------------------
module Kafka.Consumer.Types
( KafkaConsumer(..)
, ConsumerGroupId(..)
, Offset(..)
, OffsetReset(..)
, RebalanceEvent(..)
, PartitionOffset(..)
, SubscribedPartitions(..)
, Timestamp(..)
, OffsetCommit(..)
, OffsetStoreSync(..)
, OffsetStoreMethod(..)
, TopicPartition(..)
, ConsumerRecord(..)
, crMapKey
, crMapValue
, crMapKV
-- why are these here?

-- * Deprecated
, sequenceFirst
, traverseFirst
, traverseFirstM
, traverseM
, bitraverseM
)
where

import Data.Bifoldable      (Bifoldable (..))
import Data.Bifunctor       (Bifunctor (..))
import Data.Bitraversable   (Bitraversable (..), bimapM, bisequence)
import Data.Int             (Int64)
import Data.String          (IsString)
import Data.Text            (Text)
import Data.Typeable        (Typeable)
import GHC.Generics         (Generic)
import Kafka.Internal.Setup (HasKafka (..), HasKafkaConf (..), Kafka (..), KafkaConf (..))
import Kafka.Types          (Millis (..), PartitionId (..), TopicName (..))

-- | The main type for Kafka consumption, used e.g. to poll and commit messages.
-- 
-- Its constructor is intentionally not exposed, instead, one should use 'Kafka.Consumer.newConsumer' to acquire such a value.
data KafkaConsumer = KafkaConsumer
  { kcKafkaPtr  :: !Kafka
  , kcKafkaConf :: !KafkaConf
  }

instance HasKafka KafkaConsumer where
  getKafka = kcKafkaPtr
  {-# INLINE getKafka #-}

instance HasKafkaConf KafkaConsumer where
  getKafkaConf = kcKafkaConf
  {-# INLINE getKafkaConf #-}

-- | Consumer group ID. Different consumers with the same consumer group ID will get assigned different partitions of each subscribed topic. 
-- 
-- See <https://kafka.apache.org/documentation/#group.id Kafka documentation on consumer group>
newtype ConsumerGroupId = ConsumerGroupId
  { unConsumerGroupId :: Text
  } deriving (Show, Ord, Eq, IsString, Generic)

-- | A message offset in a partition
newtype Offset          = Offset { unOffset :: Int64 } deriving (Show, Eq, Ord, Read, Generic)

-- | Where to reset the offset when there is no initial offset in Kafka
--
-- See <https://kafka.apache.org/documentation/#auto.offset.reset Kafka documentation on offset reset>
data OffsetReset        = Earliest | Latest deriving (Show, Eq, Generic)

-- | A set of events which happen during the rebalancing process
data RebalanceEvent =
    -- | Happens before Kafka Client confirms new assignment
    RebalanceBeforeAssign [TopicPartition]
    -- | Happens after the new assignment is confirmed
  | RebalanceAssign [TopicPartition]
    -- | Happens before Kafka Client confirms partitions rejection
  | RebalanceBeforeRevoke [TopicPartition]
    -- | Happens after the rejection is confirmed
  | RebalanceRevoke [TopicPartition]
  deriving (Eq, Show, Generic)

-- | The partition offset
data PartitionOffset =
    PartitionOffsetBeginning
  | PartitionOffsetEnd
  | PartitionOffset Int64
  | PartitionOffsetStored
  | PartitionOffsetInvalid
  deriving (Eq, Show, Generic)

-- | Partitions subscribed by a consumer
data SubscribedPartitions
  = SubscribedPartitions [PartitionId] -- ^ Subscribe only to those partitions
  | SubscribedPartitionsAll            -- ^ Subscribe to all partitions
  deriving (Show, Eq, Generic)

-- | Consumer record timestamp 
data Timestamp =
    CreateTime !Millis
  | LogAppendTime !Millis
  | NoTimestamp
  deriving (Show, Eq, Read, Generic)

-- | Offsets commit mode
data OffsetCommit =
      OffsetCommit       -- ^ Forces consumer to block until the broker offsets commit is done
    | OffsetCommitAsync  -- ^ Offsets will be committed in a non-blocking way
    deriving (Show, Eq, Generic)


-- | Indicates how offsets are to be synced to disk
data OffsetStoreSync =
      OffsetSyncDisable       -- ^ Do not sync offsets (in Kafka: -1)
    | OffsetSyncImmediate     -- ^ Sync immediately after each offset commit (in Kafka: 0)
    | OffsetSyncInterval Int  -- ^ Sync after specified interval in millis
    deriving (Show, Eq, Generic)

-- | Indicates the method of storing the offsets
data OffsetStoreMethod =
      OffsetStoreBroker                         -- ^ Offsets are stored in Kafka broker (preferred)
    | OffsetStoreFile FilePath OffsetStoreSync  -- ^ Offsets are stored in a file (and synced to disk according to the sync policy)
    deriving (Show, Eq, Generic)

-- | Kafka topic partition structure
data TopicPartition = TopicPartition
  { tpTopicName :: TopicName
  , tpPartition :: PartitionId
  , tpOffset    :: PartitionOffset
  } deriving (Show, Eq, Generic)

-- | Represents a /received/ message from Kafka (i.e. used in a consumer)
data ConsumerRecord k v = ConsumerRecord
  { crTopic     :: !TopicName    -- ^ Kafka topic this message was received from
  , crPartition :: !PartitionId  -- ^ Kafka partition this message was received from
  , crOffset    :: !Offset       -- ^ Offset within the 'crPartition' Kafka partition
  , crTimestamp :: !Timestamp    -- ^ Message timestamp
  , crKey       :: !k            -- ^ Message key
  , crValue     :: !v            -- ^ Message value
  }
  deriving (Eq, Show, Read, Typeable, Generic)

instance Bifunctor ConsumerRecord where
  bimap f g (ConsumerRecord t p o ts k v) =  ConsumerRecord t p o ts (f k) (g v)
  {-# INLINE bimap #-}

instance Functor (ConsumerRecord k) where
  fmap = second
  {-# INLINE fmap #-}

instance Foldable (ConsumerRecord k) where
  foldMap f r = f (crValue r)
  {-# INLINE foldMap #-}

instance Traversable (ConsumerRecord k) where
  traverse f r = (\v -> crMapValue (const v) r) <$> f (crValue r)
  {-# INLINE traverse #-}

instance Bifoldable ConsumerRecord where
  bifoldMap f g r = f (crKey r) `mappend` g (crValue r)
  {-# INLINE bifoldMap #-}

instance Bitraversable ConsumerRecord where
  bitraverse f g r = (\k v -> bimap (const k) (const v) r) <$> f (crKey r) <*> g (crValue r)
  {-# INLINE bitraverse #-}

{-# DEPRECATED crMapKey "Isn't concern of this library. Use 'first'" #-}
crMapKey :: (k -> k') -> ConsumerRecord k v -> ConsumerRecord k' v
crMapKey = first
{-# INLINE crMapKey #-}

{-# DEPRECATED crMapValue "Isn't concern of this library. Use 'second'" #-}
crMapValue :: (v -> v') -> ConsumerRecord k v -> ConsumerRecord k v'
crMapValue = second
{-# INLINE crMapValue #-}

{-# DEPRECATED crMapKV "Isn't concern of this library. Use 'bimap'" #-}
crMapKV :: (k -> k') -> (v -> v') -> ConsumerRecord k v -> ConsumerRecord k' v'
crMapKV = bimap
{-# INLINE crMapKV #-}

{-# DEPRECATED sequenceFirst "Isn't concern of this library. Use @'bitraverse' 'id' 'pure'@" #-}
sequenceFirst :: (Bitraversable t, Applicative f) => t (f k) v -> f (t k v)
sequenceFirst = bitraverse id pure
{-# INLINE sequenceFirst #-}

{-# DEPRECATED traverseFirst "Isn't concern of this library. Use @'bitraverse' f 'pure'@"  #-}
traverseFirst :: (Bitraversable t, Applicative f)
              => (k -> f k')
              -> t k v
              -> f (t k' v)
traverseFirst f = bitraverse f pure
{-# INLINE traverseFirst #-}

{-# DEPRECATED traverseFirstM "Isn't concern of this library. Use @'bitraverse' 'id' 'pure' '<$>' 'bitraverse' f 'pure' r@"  #-}
traverseFirstM :: (Bitraversable t, Applicative f, Monad m)
               => (k -> m (f k'))
               -> t k v
               -> m (f (t k' v))
traverseFirstM f r = bitraverse id pure <$> bitraverse f pure r
{-# INLINE traverseFirstM #-}

{-# DEPRECATED traverseM "Isn't concern of this library. Use @'sequenceA' '<$>' 'traverse' f r@"  #-}
traverseM :: (Traversable t, Applicative f, Monad m)
          => (v -> m (f v'))
          -> t v
          -> m (f (t v'))
traverseM f r = sequenceA <$> traverse f r
{-# INLINE traverseM #-}

{-# DEPRECATED bitraverseM "Isn't concern of this library. Use @'Data.Bitraversable.bisequenceA' '<$>' 'bimapM' f g r@"  #-}
bitraverseM :: (Bitraversable t, Applicative f, Monad m)
            => (k -> m (f k'))
            -> (v -> m (f v'))
            -> t k v
            -> m (f (t k' v'))
bitraverseM f g r = bisequence <$> bimapM f g r
{-# INLINE bitraverseM #-}

