{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module holding producer types.
-----------------------------------------------------------------------------
module Kafka.Producer.Types
( KafkaProducer(..)
, ProducerRecord(..)
, ProducePartition(..)
, DeliveryReport(..)
, ImmediateError(..)
)
where

import Data.ByteString
import Data.Typeable        (Typeable)
import GHC.Generics         (Generic)
import Kafka.Consumer.Types (Offset (..))
import Kafka.Internal.Setup (HasKafka (..), HasKafkaConf (..), HasTopicConf (..), Kafka (..), KafkaConf (..), TopicConf (..))
import Kafka.Types          (KafkaError (..), TopicName (..))

-- | The main type for Kafka message production, used e.g. to send messages.
--
-- Its constructor is intentionally not exposed, instead, one should used 'Kafka.Producer.newProducer' to acquire such a value.
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

-- | 
data ProducePartition =
    -- | The partition number of the topic
    SpecifiedPartition {-# UNPACK #-} !Int
    -- | Let the Kafka broker decide the partition
  | UnassignedPartition
  deriving (Show, Eq, Ord, Typeable, Generic)

-- | Data type representing an error that is caused by pre-flight conditions not being met
newtype ImmediateError = ImmediateError KafkaError
  deriving newtype (Eq, Show)

-- | The result of sending a message to the broker, useful for callbacks
data DeliveryReport
    -- | The message was successfully sent at this offset
  = DeliverySuccess ProducerRecord Offset
    -- | The message could not be sent
  | DeliveryFailure ProducerRecord KafkaError
    -- | An error occurred, but /librdkafka/ did not attach any sent message
  | NoMessageError KafkaError
  deriving (Show, Eq, Generic)
