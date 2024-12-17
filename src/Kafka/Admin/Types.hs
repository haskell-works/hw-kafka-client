module Kafka.Admin.Types (
KAdmin(..)
, PartitionCount (..)
, ReplicationFactor (..)
, NewTopic (..)
) where

import Data.Map

import Kafka.Types
import Kafka.Internal.Setup

data KAdmin = KAdmin {
  kaKafkaPtr  :: !Kafka
  , kaKafkaConf :: !KafkaConf
}

instance HasKafka KAdmin where
  getKafka = kaKafkaPtr
  {-# INLINE getKafka #-}

instance HasKafkaConf KAdmin where
  getKafkaConf = kaKafkaConf
  {-# INLINE getKafkaConf #-}

newtype PartitionCount = PartitionCount { unPartitionCount :: Int } deriving (Show, Eq)
newtype ReplicationFactor = ReplicationFactor { unReplicationFactor :: Int } deriving (Show, Eq)

data NewTopic = NewTopic {
  topicName :: TopicName
  , topicPartitionCount :: PartitionCount
  , topicReplicationFactor :: ReplicationFactor
  , topicConfig :: Map String String
} deriving (Show)
