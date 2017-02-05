{-# LANGUAGE DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
module Kafka.Consumer.Types

where

import Data.Bifunctor
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
  { messageTopic     :: !TopicName
    -- | Kafka partition this message was received from
  , messagePartition :: !PartitionId
    -- | Offset within the 'messagePartition' Kafka partition
  , messageOffset    :: !Offset
  , messageKey       :: !k
  , messagePayload   :: !v
  }
  deriving (Eq, Show, Read, Typeable)

instance Bifunctor ConsumerRecord where
  bimap f g (ConsumerRecord t p o k v) =  ConsumerRecord t p o (f k) (g v)

instance Functor (ConsumerRecord k) where
  fmap = second

crMapKey :: (k -> k') -> ConsumerRecord k v -> ConsumerRecord k' v
crMapKey = first

crMapValue :: (v -> v') -> ConsumerRecord k v -> ConsumerRecord k v'
crMapValue = second

crMapKV :: (k -> k') -> (v -> v') -> ConsumerRecord k v -> ConsumerRecord k' v'
crMapKV = bimap

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
