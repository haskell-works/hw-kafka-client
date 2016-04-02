{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Metadata.Internal.Types
where

import           Data.Typeable
import           Kafka

-- | Metadata for all Kafka brokers
data KafkaMetadata = KafkaMetadata
    {
    -- | Broker metadata
      brokers :: [KafkaBrokerMetadata]
    -- | topic metadata
    , topics  :: [Either KafkaError KafkaTopicMetadata]
    }
  deriving (Eq, Show, Typeable)

-- | Metadata for a specific Kafka broker
data KafkaBrokerMetadata = KafkaBrokerMetadata
    {
    -- | broker identifier
      brokerId   :: Int
    -- | hostname for the broker
    , brokerHost :: String
    -- | port for the broker
    , brokerPort :: Int
    }
  deriving (Eq, Show, Typeable)

-- | Metadata for a specific topic
data KafkaTopicMetadata = KafkaTopicMetadata
    {
    -- | name of the topic
      topicName       :: String
    -- | partition metadata
    , topicPartitions :: [Either KafkaError KafkaPartitionMetadata]
    } deriving (Eq, Show, Typeable)

-- | Metadata for a specific partition
data KafkaPartitionMetadata = KafkaPartitionMetadata
    {
    -- | identifier for the partition
      partitionId       :: Int

    -- | broker leading this partition
    , partitionLeader   :: Int

    -- | replicas of the leader
    , partitionReplicas :: [Int]

    -- | In-sync replica set, see <http://kafka.apache.org/documentation.html>
    , partitionIsrs     :: [Int]
    }
  deriving (Eq, Show, Typeable)
