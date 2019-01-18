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

newtype BrokerId = BrokerId { unBrokerId :: Int } deriving (Show, Eq, Ord, Read, Generic)

newtype PartitionId = PartitionId { unPartitionId :: Int } deriving (Show, Eq, Read, Ord, Enum, Generic)
newtype Millis      = Millis { unMillis :: Int64 } deriving (Show, Read, Eq, Ord, Num, Generic)
newtype ClientId    = ClientId { unClientId :: Text } deriving (Show, Eq, Ord, Generic)
newtype BatchSize   = BatchSize { unBatchSize :: Int } deriving (Show, Read, Eq, Ord, Num, Generic)

data TopicType =
    User    -- ^ Normal topics that are created by user.
  | System  -- ^ Topics starting with "__" (__consumer_offsets, __confluent.support.metrics) are considered "system" topics
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

--
-- | Any Kafka errors
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

data KafkaCompressionCodec =
    NoCompression
  | Gzip
  | Snappy
  | Lz4
  deriving (Eq, Show, Typeable, Generic)

kafkaCompressionCodecToText :: KafkaCompressionCodec -> Text
kafkaCompressionCodecToText c = case c of
  NoCompression -> "none"
  Gzip          -> "gzip"
  Snappy        -> "snappy"
  Lz4           -> "lz4"
