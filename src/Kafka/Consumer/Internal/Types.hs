{-# LANGUAGE DeriveDataTypeable #-}

module Kafka.Consumer.Internal.Types

where

import qualified Data.ByteString as BS
import           Data.Int
import           Data.Typeable
import           Kafka

newtype ConsumerGroupId = ConsumerGroupId String deriving (Show, Eq)

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
  , tpPartition :: Int
  , tpOffset    :: PartitionOffset } deriving (Show, Eq)

-- | Represents /received/ messages from a Kafka broker (i.e. used in a consumer)
data ReceivedMessage = ReceivedMessage
  { messageTopic     :: !String
    -- | Kafka partition this message was received from
  , messagePartition :: !Int
    -- | Offset within the 'messagePartition' Kafka partition
  , messageOffset    :: !Int64
    -- | Contents of the message, as a 'ByteString'
  , messagePayload   :: !BS.ByteString
    -- | Optional key of the message. 'Nothing' when the message
    -- was enqueued without a key
  , messageKey       :: Maybe BS.ByteString
  }
  deriving (Eq, Show, Read, Typeable)

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
