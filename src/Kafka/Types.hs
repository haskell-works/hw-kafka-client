{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
module Kafka.Types
( BrokerId(..)
, PartitionId(..)
, Millis(..)
, ClientId(..)
, BatchSize(..)
, TopicName(..)
, BrokerAddress(..)
, Timeout(..)
, KafkaLogLevel(..)
, KafkaError(..)
, KafkaDebug(..)
, KafkaCompressionCodec(..)
, TopicType(..)
, topicType
, kafkaDebugToText
, kafkaCompressionCodecToText
)
where

import Control.Exception      (Exception (..))
import Data.Int               (Int64)
import Data.Text              (Text, isPrefixOf)
import Data.Typeable          (Typeable)
import GHC.Generics           (Generic)
import Kafka.Internal.RdKafka (RdKafkaRespErrT, rdKafkaErr2name, rdKafkaErr2str)

-- | Kafka broker ID
newtype BrokerId = BrokerId { unBrokerId :: Int } deriving (Show, Eq, Ord, Read, Generic)

-- | Topic partition ID
newtype PartitionId = PartitionId { unPartitionId :: Int } deriving (Show, Eq, Read, Ord, Enum, Generic)

-- | A number of milliseconds, used to represent durations and timestamps
newtype Millis      = Millis { unMillis :: Int64 } deriving (Show, Read, Eq, Ord, Num, Generic)

-- | Client ID used by Kafka to better track requests
-- 
-- See <https://kafka.apache.org/documentation/#client.id Kafka documentation on client ID>
newtype ClientId    = ClientId { unClientId :: Text } deriving (Show, Eq, Ord, Generic)

-- | Batch size used for polling
newtype BatchSize   = BatchSize { unBatchSize :: Int } deriving (Show, Read, Eq, Ord, Num, Generic)

-- | Whether the topic is created by a user or by the system
data TopicType =
    User    -- ^ Normal topics that are created by user.
  | System  -- ^ Topics starting with a double underscore "\__" (@__consumer_offsets@, @__confluent.support.metrics@, etc.) are considered "system" topics
  deriving (Show, Read, Eq, Ord, Generic)

-- | Topic name to be consumed
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName =
    TopicName { unTopicName :: Text } -- ^ a simple topic name or a regex if started with @^@
    deriving (Show, Eq, Ord, Read, Generic)

-- | Deduce the type of a topic from its name, by checking if it starts with a double underscore "\__"
topicType :: TopicName -> TopicType
topicType (TopicName tn) =
  if "__" `isPrefixOf` tn then System else User
{-# INLINE topicType #-}

-- | Kafka broker address string (e.g. @broker1:9092@)
newtype BrokerAddress = BrokerAddress { unBrokerAddress :: Text } deriving (Show, Eq, Generic)

-- | Timeout in milliseconds
newtype Timeout = Timeout { unTimeout :: Int } deriving (Show, Eq, Read, Generic)

-- | Log levels for /librdkafka/.
data KafkaLogLevel =
  KafkaLogEmerg | KafkaLogAlert | KafkaLogCrit | KafkaLogErr | KafkaLogWarning |
  KafkaLogNotice | KafkaLogInfo | KafkaLogDebug
  deriving (Show, Enum, Eq)

-- | All possible Kafka errors
data KafkaError =
    KafkaError Text
  | KafkaInvalidReturnValue
  | KafkaBadSpecification Text
  | KafkaResponseError RdKafkaRespErrT
  | KafkaInvalidConfigurationValue Text
  | KafkaUnknownConfigurationKey Text
  | KafkaBadConfiguration
    deriving (Eq, Show, Typeable, Generic)

instance Exception KafkaError where
  displayException (KafkaResponseError err) =
    "[" ++ rdKafkaErr2name err ++ "] " ++ rdKafkaErr2str err
  displayException err = show err

-- | Available /librdkafka/ debug contexts
data KafkaDebug =
    DebugGeneric
  | DebugBroker
  | DebugTopic
  | DebugMetadata
  | DebugQueue
  | DebugMsg
  | DebugProtocol
  | DebugCgrp
  | DebugSecurity
  | DebugFetch
  | DebugFeature
  | DebugAll
  deriving (Eq, Show, Typeable, Generic)

-- | Convert a 'KafkaDebug' into its /librdkafka/ string equivalent.
--
-- This is used internally by the library but may be useful to some developers. 
kafkaDebugToText :: KafkaDebug -> Text
kafkaDebugToText d = case d of
  DebugGeneric  -> "generic"
  DebugBroker   -> "broker"
  DebugTopic    -> "topic"
  DebugMetadata -> "metadata"
  DebugQueue    -> "queue"
  DebugMsg      -> "msg"
  DebugProtocol -> "protocol"
  DebugCgrp     -> "cgrp"
  DebugSecurity -> "security"
  DebugFetch    -> "fetch"
  DebugFeature  -> "feature"
  DebugAll      -> "all"

-- | Compression codec used by a topic
--
-- See <https://kafka.apache.org/documentation/#compression.type Kafka documentation on compression codecs>
data KafkaCompressionCodec =
    NoCompression
  | Gzip
  | Snappy
  | Lz4
  deriving (Eq, Show, Typeable, Generic)

-- | Convert a 'KafkaCompressionCodec' into its /librdkafka/ string equivalent.
--
-- This is used internally by the library but may be useful to some developers.
kafkaCompressionCodecToText :: KafkaCompressionCodec -> Text
kafkaCompressionCodecToText c = case c of
  NoCompression -> "none"
  Gzip          -> "gzip"
  Snappy        -> "snappy"
  Lz4           -> "lz4"
