{-# LANGUAGE DeriveDataTypeable #-}

module Kafka.Consumer.Internal.Types

where

import qualified Data.ByteString      as BS
import           Data.Int
import           Data.Typeable
import           Kafka.Internal.Types

newtype ConsumerGroupId = ConsumerGroupId String deriving (Show, Eq)

-- | Topic name to be consumed
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName =
    TopicName String -- ^ a simple topic name or a regex if started with @^@
    deriving (Show, Eq)

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
data KafkaTopicPartition = KafkaTopicPartition
  { ktpTopicName :: TopicName
  , ktpPartition :: Int
  , ktpOffset    :: KafkaOffset } deriving (Show, Eq)

-- | Represents /received/ messages from a Kafka broker (i.e. used in a consumer)
data KafkaMessage =
  KafkaMessage {
                  messageTopic     :: !String
                 -- | Kafka partition this message was received from
               ,  messagePartition :: !Int
                 -- | Offset within the 'messagePartition' Kafka partition
               , messageOffset     :: !Int64
                 -- | Contents of the message, as a 'ByteString'
               , messagePayload    :: !BS.ByteString
                 -- | Optional key of the message. 'Nothing' when the message
                 -- was enqueued without a key
               , messageKey        :: Maybe BS.ByteString
               }
  deriving (Eq, Show, Read, Typeable)

