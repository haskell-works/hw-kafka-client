{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Metadata.Internal.Types
where

import           Data.Typeable
import           Kafka

-- | Metadata for all Kafka brokers
data KafkaMetadata = KafkaMetadata
    {
    -- | Broker metadata
      brokers :: [BrokerMetadata]
    -- | topic metadata
    , topics  :: [Either KafkaError TopicMetadata]
    }
  deriving (Eq, Show, Typeable)

-- | Metadata for a specific Kafka broker
data BrokerMetadata = BrokerMetadata
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
data TopicMetadata = TopicMetadata
    {
    -- | name of the topic
      topicName       :: String
    -- | partition metadata
    , topicPartitions :: [Either KafkaError PartitionMetadata]
    } deriving (Eq, Show, Typeable)

-- | Metadata for a specific partition
data PartitionMetadata = PartitionMetadata
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
