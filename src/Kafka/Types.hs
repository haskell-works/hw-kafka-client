{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Types
where

import Control.Exception
import Data.Typeable
import Kafka.Internal.RdKafka

class HasKafka a where
  getKafka :: a -> Kafka

class HasKafkaConf a where
  getKafkaConf :: a -> KafkaConf

newtype BrokerId =
  BrokerId Int
  deriving (Show, Eq, Ord, Read)

newtype Kafka     = Kafka RdKafkaTPtr deriving Show
newtype KafkaConf = KafkaConf RdKafkaConfTPtr deriving Show
newtype TopicConf = TopicConf RdKafkaTopicConfTPtr deriving Show

instance HasKafkaConf KafkaConf where
  getKafkaConf = id
  {-# INLINE getKafkaConf #-}

instance HasKafka Kafka where
  getKafka = id
  {-# INLINE getKafka #-}

-- | Topic name to be consumed
--
-- Wildcard (regex) topics are supported by the /librdkafka/ assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName =
    TopicName String -- ^ a simple topic name or a regex if started with @^@
    deriving (Show, Eq, Ord, Read)

-- | Kafka broker address string (e.g. @broker1:9092@)
newtype BrokerAddress = BrokerAddress String deriving (Show, Eq)

-- | Timeout in milliseconds
newtype Timeout = Timeout Int deriving (Show, Eq, Read)

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
    KafkaError String
  | KafkaInvalidReturnValue
  | KafkaBadSpecification String
  | KafkaResponseError RdKafkaRespErrT
  | KafkaInvalidConfigurationValue String
  | KafkaUnknownConfigurationKey String
  | KakfaBadConfiguration
    deriving (Eq, Show, Typeable)

instance Exception KafkaError

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

kafkaDebugToString :: KafkaDebug -> String
kafkaDebugToString d =case d of
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

kafkaCompressionCodecToString :: KafkaCompressionCodec -> String
kafkaCompressionCodecToString c = case c of
  NoCompression -> "none"
  Gzip          -> "gzip"
  Snappy        -> "snappy"
  Lz4           -> "lz4"
