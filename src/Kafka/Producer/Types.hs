{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
module Kafka.Producer.Types
( KafkaProducer(..)
, ProducerRecord(..)
, ProducePartition(..)
, DeliveryReport(..)
)
where

import Data.ByteString
import Data.Typeable        (Typeable)
import GHC.Generics         (Generic)
import Kafka.Consumer.Types (Offset (..))
import Kafka.Internal.Setup (HasKafka (..), HasKafkaConf (..), HasTopicConf (..), Kafka (..), KafkaConf (..), TopicConf (..))
import Kafka.Types          (KafkaError (..), TopicName (..))

-- | Main pointer to Kafka object, which contains our brokers
data KafkaProducer = KafkaProducer
  { kpKafkaPtr  :: !Kafka
  , kpKafkaConf :: !KafkaConf
  , kpTopicConf :: !TopicConf
  }

instance HasKafka KafkaProducer where
  getKafka = kpKafkaPtr
  {-# INLINE getKafka #-}

instance HasKafkaConf KafkaProducer where
  getKafkaConf = kpKafkaConf
  {-# INLINE getKafkaConf #-}

instance HasTopicConf KafkaProducer where
  getTopicConf = kpTopicConf
  {-# INLINE getTopicConf #-}

-- | Represents messages /to be enqueued/ onto a Kafka broker (i.e. used for a producer)
data ProducerRecord = ProducerRecord
  { prTopic     :: !TopicName
  , prPartition :: !ProducePartition
  , prKey       :: Maybe ByteString
  , prValue     :: Maybe ByteString
  } deriving (Eq, Show, Typeable, Generic)

data ProducePartition =
    SpecifiedPartition {-# UNPACK #-} !Int  -- the partition number of the topic
  | UnassignedPartition
  deriving (Show, Eq, Ord, Typeable, Generic)

data DeliveryReport
  = DeliverySuccess ProducerRecord Offset
  | DeliveryFailure ProducerRecord KafkaError
  | NoMessageError KafkaError
  deriving (Show, Eq, Generic)
