module Kafka.Topic.Types (
PartitionCount (..)
, ReplicationFactor (..)
, NewTopic (..)
) where

import Data.Map

import Kafka.Types

newtype PartitionCount = PartitionCount { unPartitionCount :: Int } deriving (Show, Eq)
newtype ReplicationFactor = ReplicationFactor { unReplicationFactor :: Int } deriving (Show, Eq)

data NewTopic = NewTopic {
  topicName :: TopicName
  , topicPartitionCount :: PartitionCount
  , topicReplicationFactor :: ReplicationFactor
  , topicConfig :: Map String String
} deriving (Show)
