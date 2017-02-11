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

instance Functor (ConsumerRecord k) where
  fmap = second

instance Foldable (ConsumerRecord k) where
  foldMap f r = f (crValue r)

instance Traversable (ConsumerRecord k) where
  traverse f r = (\v -> crMapValue (const v) r) <$> f (crValue r)

instance Bifoldable ConsumerRecord where
  bifoldMap f g r = f (crKey r) `mappend` g (crValue r)

instance Bitraversable ConsumerRecord where
  bitraverse f g r =
    (\k v -> bimap (const k) (const v) r) <$> f (crKey r) <*> g (crValue r)

crMapKey :: (k -> k') -> ConsumerRecord k v -> ConsumerRecord k' v
crMapKey = first
{-# INLINE crMapKey #-}

crMapValue :: (v -> v') -> ConsumerRecord k v -> ConsumerRecord k v'
crMapValue = second
{-# INLINE crMapValue #-}

crMapKV :: (k -> k') -> (v -> v') -> ConsumerRecord k v -> ConsumerRecord k' v'
crMapKV = bimap
{-# INLINE crMapKV #-}

crSequenceKey :: Functor t => ConsumerRecord (t k) v -> t (ConsumerRecord k v)
crSequenceKey cr = (\k -> crMapKey (const k) cr) <$> crKey cr
{-# INLINE crSequenceKey #-}

crSequenceValue :: Functor t => ConsumerRecord k (t v) -> t (ConsumerRecord k v)
crSequenceValue cr = (\v -> crMapValue (const v) cr) <$> crValue cr
{-# INLINE crSequenceValue #-}

crSequenceKV :: Applicative t => ConsumerRecord (t k) (t v) -> t (ConsumerRecord k v)
crSequenceKV cr =
  (\k v -> bimap (const k) (const v) cr) <$> crKey cr <*> crValue cr
{-# INLINE crSequenceKV #-}

crTraverseKey :: Functor t
              => (k -> t k')
              -> ConsumerRecord k v
              -> t (ConsumerRecord k' v)
crTraverseKey f r = (\k -> crMapKey (const k) r) <$> f (crKey r)
{-# INLINE crTraverseKey #-}

crTraverseValue :: Functor t
                => (v -> t v')
                -> ConsumerRecord k v
                -> t (ConsumerRecord k v')
crTraverseValue f r = (\v -> crMapValue (const v) r) <$> f (crValue r)
{-# INLINE crTraverseValue #-}

crTraverseKV :: Applicative t
             => (k -> t k')
             -> (v -> t v')
             -> ConsumerRecord k v
             -> t (ConsumerRecord k' v')
crTraverseKV = bitraverse
{-# INLINE crTraverseKV #-}

crTraverseKeyM :: (Functor t, Monad m)
               => (k -> m (t k'))
               -> ConsumerRecord k v
               -> m (t (ConsumerRecord k' v))
crTraverseKeyM f r = do
  res <- f (crKey r)
  return $ (\x -> first (const x) r) <$> res
{-# INLINE crTraverseKeyM #-}

crTraverseValueM :: (Functor t, Monad m)
               => (v -> m (t v'))
               -> ConsumerRecord k v
               -> m (t (ConsumerRecord k v'))
crTraverseValueM f r = do
  res <- f (crValue r)
  return $ (\x -> second (const x) r) <$> res
{-# INLINE crTraverseValueM #-}

crTraverseKVM :: (Applicative t, Monad m)
              => (k -> m (t k'))
              -> (v -> m (t v'))
              -> ConsumerRecord k v
              -> m (t (ConsumerRecord k' v'))
crTraverseKVM f g r = do
  keyRes <- f (crKey r)
  valRes <- g (crValue r)
  return $ (\k v -> bimap (const k) (const v) r) <$> keyRes <*> valRes
{-# INLINE crTraverseKVM #-}

data PartitionOffset =
  -- | Start reading from the beginning of the partition
    PartitionOffsetBeginning

  -- | Start reading from the end
  | PartitionOffsetEnd

  -- | Start reading from a specific location within the partition
  | PartitionOffset Int64

  -- | Start reading from the stored offset. See
  -- <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md librdkafka's documentation>
  -- for offset store configuration.
  | PartitionOffsetStored
  | PartitionOffsetInvalid
  deriving (Eq, Show)
