{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
module Kafka.Consumer.Types

where

import Data.Bifunctor
import Data.Bifoldable
import Data.Bitraversable
import Data.Int
import Data.Typeable
import Kafka.Types
import Kafka.Internal.RdKafka

data KafkaConsumer = KafkaConsumer { kcKafkaPtr :: !RdKafkaTPtr, kcKafkaConf :: !RdKafkaConfTPtr} deriving (Show)

newtype ReballanceCallback = ReballanceCallback (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ())
newtype OffsetsCommitCallback = OffsetsCommitCallback (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ())

newtype ConsumerGroupId = ConsumerGroupId String deriving (Show, Eq)
newtype Offset          = Offset Int64 deriving (Show, Eq, Read)
newtype PartitionId     = PartitionId Int deriving (Show, Eq, Read)
newtype Millis          = Millis Int deriving (Show, Eq, Ord, Num)
newtype ClientId        = ClientId String deriving (Show, Eq, Ord)
data OffsetReset        = Earliest | Latest deriving (Show, Eq)

-- | Offsets commit mode
data OffsetCommit =
      OffsetCommit       -- ^ Forces consumer to block until the broker offsets commit is done
    | OffsetCommitAsync  -- ^ Offsets will be committed in a non-blocking way
    deriving (Show, Eq)


-- | Indicates how offsets are to be synced to disk
data OffsetStoreSync =
      OffsetSyncDisable       -- ^ Do not sync offsets (in Kafka: -1)
    | OffsetSyncImmediate     -- ^ Sync immediately after each offset commit (in Kafka: 0)
    | OffsetSyncInterval Int  -- ^ Sync after specified interval in millis

-- | Indicates the method of storing the offsets
data OffsetStoreMethod =
      OffsetStoreBroker                         -- ^ Offsets are stored in Kafka broker (preferred)
    | OffsetStoreFile FilePath OffsetStoreSync  -- ^ Offsets are stored in a file (and synced to disk according to the sync policy)

-- | Kafka topic partition structure
data TopicPartition = TopicPartition
  { tpTopicName :: TopicName
  , tpPartition :: PartitionId
  , tpOffset    :: PartitionOffset } deriving (Show, Eq)

-- | Represents /received/ messages from a Kafka broker (i.e. used in a consumer)
data ConsumerRecord k v = ConsumerRecord
  { crTopic     :: !TopicName
    -- | Kafka partition this message was received from
  , crPartition :: !PartitionId
    -- | Offset within the 'crPartition' Kafka partition
  , crOffset    :: !Offset
  , crKey       :: !k
  , crValue     :: !v
  }
  deriving (Eq, Show, Read, Typeable)

instance Bifunctor ConsumerRecord where
  bimap f g (ConsumerRecord t p o k v) =  ConsumerRecord t p o (f k) (g v)
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

crMapKey :: (k -> k') -> ConsumerRecord k v -> ConsumerRecord k' v
crMapKey = first
{-# INLINE crMapKey #-}

crMapValue :: (v -> v') -> ConsumerRecord k v -> ConsumerRecord k v'
crMapValue = second
{-# INLINE crMapValue #-}

crMapKV :: (k -> k') -> (v -> v') -> ConsumerRecord k v -> ConsumerRecord k' v'
crMapKV = bimap
{-# INLINE crMapKV #-}

sequenceFirst :: (Bitraversable t, Applicative f) => t (f k) v -> f (t k v)
sequenceFirst = bitraverse id pure
{-# INLINE sequenceFirst #-}

traverseFirst :: (Bitraversable t, Applicative f)
              => (k -> f k')
              -> t k v
              -> f (t k' v)
traverseFirst f = bitraverse f pure
{-# INLINE traverseFirst #-}

traverseFirstM :: (Bitraversable t, Applicative f, Monad m)
               => (k -> m (f k'))
               -> t k v
               -> m (f (t k' v))
traverseFirstM f r = bitraverse id pure <$> bitraverse f pure r
{-# INLINE traverseFirstM #-}

traverseM :: (Traversable t, Applicative f, Monad m)
          => (v -> m (f v'))
          -> t v
          -> m (f (t v'))
traverseM f r = sequenceA <$> traverse f r
{-# INLINE traverseM #-}

bitraverseM :: (Bitraversable t, Applicative f, Monad m)
            => (k -> m (f k'))
            -> (v -> m (f v'))
            -> t k v
            -> m (f (t k' v'))
bitraverseM f g r = bisequenceA <$> bimapM f g r
{-# INLINE bitraverseM #-}

data PartitionOffset =
    PartitionOffsetBeginning
  | PartitionOffsetEnd
  | PartitionOffset Int64
  | PartitionOffsetStored
  | PartitionOffsetInvalid
  deriving (Eq, Show)
