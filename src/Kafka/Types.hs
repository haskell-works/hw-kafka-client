{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Kafka.Types
where

import Control.Exception (Exception(..))
import Data.Int (Int64)
import Data.Typeable (Typeable)
import Kafka.Internal.RdKafka (RdKafkaRespErrT, rdKafkaErr2name, rdKafkaErr2str)
import Data.Text (Text)

newtype BrokerId = BrokerId { unBrokerId :: Int } deriving (Show, Eq, Ord, Read)

newtype PartitionId = PartitionId { unPartitionId :: Int } deriving (Show, Eq, Read, Ord, Enum)
newtype Millis      = Millis { unMillis :: Int64 } deriving (Show, Read, Eq, Ord, Num)
newtype ClientId    = ClientId { unClientId :: Text } deriving (Show, Eq, Ord)
newtype BatchSize   = BatchSize { unBatchSize :: Int } deriving (Show, Read, Eq, Ord, Num)

-- | Topic name to be consumed
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName =
    TopicName { unTopicName :: Text } -- ^ a simple topic name or a regex if started with @^@
    deriving (Show, Eq, Ord, Read)

-- | Kafka broker address string (e.g. @broker1:9092@)
newtype BrokerAddress = BrokerAddress { unBrokerAddress :: Text } deriving (Show, Eq)

-- | Timeout in milliseconds
newtype Timeout = Timeout { unTimeout :: Int } deriving (Show, Eq, Read)

-- | Log levels for /librdkafka/.
data KafkaLogLevel =
  KafkaLogEmerg | KafkaLogAlert | KafkaLogCrit | KafkaLogErr | KafkaLogWarning |
  KafkaLogNotice | KafkaLogInfo | KafkaLogDebug
  deriving (Show, Eq)

instance Enum KafkaLogLevel where
   toEnum 0 = KafkaLogEmerg
   toEnum 1 = KafkaLogAlert
   toEnum 2 = KafkaLogCrit
   toEnum 3 = KafkaLogErr
   toEnum 4 = KafkaLogWarning
   toEnum 5 = KafkaLogNotice
   toEnum 6 = KafkaLogInfo
   toEnum 7 = KafkaLogDebug
   toEnum _ = undefined

   fromEnum KafkaLogEmerg   = 0
   fromEnum KafkaLogAlert   = 1
   fromEnum KafkaLogCrit    = 2
   fromEnum KafkaLogErr     = 3
   fromEnum KafkaLogWarning = 4
   fromEnum KafkaLogNotice  = 5
   fromEnum KafkaLogInfo    = 6
   fromEnum KafkaLogDebug   = 7

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
    deriving (Eq, Show, Typeable)

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
  deriving (Eq, Show, Typeable)

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
  deriving (Eq, Show, Typeable)

kafkaCompressionCodecToText :: KafkaCompressionCodec -> Text
kafkaCompressionCodecToText c = case c of
  NoCompression -> "none"
  Gzip          -> "gzip"
  Snappy        -> "snappy"
  Lz4           -> "lz4"
