{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Producer.Types

where

import qualified Data.ByteString as BS
import           Data.Typeable
import           Kafka.Types
import           Kafka.Internal.RdKafka

-- | Main pointer to Kafka object, which contains our brokers
data KafkaProducer = KafkaProducer
  { kpKafkaPtr  :: RdKafkaTPtr
  , kpKafkaConf :: RdKafkaConfTPtr
  , kpTopicConf :: RdKafkaTopicConfTPtr
  } deriving (Show)

-- | Represents messages /to be enqueued/ onto a Kafka broker (i.e. used for a producer)
data ProducerRecord =
    -- | A message without a key, assigned to 'SpecifiedPartition' or 'UnassignedPartition'
    ProducerRecord
                     !TopicName
                     !ProducePartition
      {-# UNPACK #-} !BS.ByteString -- message payload

    -- | A message with a key, assigned to a partition based on the key
  | KeyedProducerRecord
                     !TopicName
      {-# UNPACK #-} !BS.ByteString -- message key
                     !ProducePartition
      {-# UNPACK #-} !BS.ByteString -- message payload
  deriving (Eq, Show, Typeable)

-- | Options for destination partition when enqueuing a message
data ProducePartition =
  -- | A specific partition in the topic
    SpecifiedPartition {-# UNPACK #-} !Int  -- the partition number of the topic

  -- | A random partition within the topic
  | UnassignedPartition
  deriving (Show, Eq, Ord, Typeable)

prTopic :: ProducerRecord -> TopicName
prTopic (ProducerRecord t _ _) = t
prTopic (KeyedProducerRecord t _ _ _) = t

prKey :: ProducerRecord -> Maybe BS.ByteString
prKey ProducerRecord{} = Nothing
prKey (KeyedProducerRecord _ k _ _) = Just k

prPartition :: ProducerRecord -> ProducePartition
prPartition (ProducerRecord _ p _) = p
prPartition (KeyedProducerRecord _ _ p _) = p
