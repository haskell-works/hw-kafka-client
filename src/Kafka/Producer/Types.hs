{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Producer.Types

where

import qualified Data.ByteString as BS
import           Data.Typeable
import           Kafka.Types

-- | Main pointer to Kafka object, which contains our brokers
data KafkaProducer = KafkaProducer
  { kpKafkaPtr  :: !Kafka
  , kpKafkaConf :: !KafkaConf
  , kpTopicConf :: !TopicConf
  } deriving (Show)

instance HasKafka KafkaProducer where
  getKafka = kpKafkaPtr
  {-# INLINE getKafka #-}

instance HasKafkaConf KafkaProducer where
  getKafkaConf = kpKafkaConf
  {-# INLINE getKafkaConf #-}

-- | Represents messages /to be enqueued/ onto a Kafka broker (i.e. used for a producer)
data ProducerRecord = ProducerRecord
  { prTopic     :: !TopicName
  , prPartition :: !ProducePartition
  , prKey       :: Maybe BS.ByteString
  , prValue     :: Maybe BS.ByteString
  } deriving (Eq, Show, Typeable)

data ProducePartition =
    SpecifiedPartition {-# UNPACK #-} !Int  -- the partition number of the topic
  | UnassignedPartition
  deriving (Show, Eq, Ord, Typeable)
