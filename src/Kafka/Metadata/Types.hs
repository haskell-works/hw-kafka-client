{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Metadata.Types
where

import           Data.Typeable
import           Kafka

-- | Metadata for all Kafka brokers
data KafkaMetadata = KafkaMetadata
    {
    -- | Broker metadata
      kmBrokers :: [BrokerMetadata]
    -- | topic metadata
    , kmTopics  :: [Either KafkaError TopicMetadata]
    }
  deriving (Eq, Show, Typeable)

-- | Metadata for a specific Kafka broker
data BrokerMetadata = BrokerMetadata
    {
    -- | broker identifier
      bmBrokerId   :: Int
    -- | hostname for the broker
    , bmBrokerHost :: String
    -- | port for the broker
    , bmBrokerPort :: Int
    }
  deriving (Eq, Show, Typeable)

-- | Metadata for a specific topic
data TopicMetadata = TopicMetadata
    {
    -- | name of the topic
      tmTopicName       :: String
    -- | partition metadata
    , tmTopicPartitions :: [Either KafkaError PartitionMetadata]
    } deriving (Eq, Show, Typeable)

-- | Metadata for a specific partition
data PartitionMetadata = PartitionMetadata
    {
    -- | identifier for the partition
      pmPartitionId       :: Int

    -- | broker leading this partition
    , pmPartitionLeader   :: Int

    -- | replicas of the leader
    , pmPartitionReplicas :: [Int]

    -- | In-sync replica set, see <http://kafka.apache.org/documentation.html>
    , pmPartitionIsrs     :: [Int]
    }
  deriving (Eq, Show, Typeable)
