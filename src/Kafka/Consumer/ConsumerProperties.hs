module Kafka.Consumer.ConsumerProperties
( module Kafka.Consumer.ConsumerProperties
, module X
)
where

--
import           Control.Monad
import qualified Data.List            as L
import           Data.Map             (Map)
import qualified Data.Map             as M
import           Kafka.Consumer.Types
import           Kafka.Internal.Setup
import           Kafka.Types

import Kafka.Consumer.Callbacks as X

-- | Properties to create 'KafkaConsumer'.
data ConsumerProperties = ConsumerProperties
  { cpProps     :: Map String String
  , cpLogLevel  :: Maybe KafkaLogLevel
  , cpCallbacks :: [KafkaConf -> IO ()]
  }

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid ConsumerProperties where
  mempty = ConsumerProperties
    { cpProps          = M.empty
    , cpLogLevel       = Nothing
    , cpCallbacks      = []
    }
  {-# INLINE mempty #-}
  mappend (ConsumerProperties m1 ll1 cb1) (ConsumerProperties m2 ll2 cb2) =
    ConsumerProperties (M.union m2 m1) (ll2 `mplus` ll1) (cb1 `mplus` cb2)
  {-# INLINE mappend #-}

brokersList :: [BrokerAddress] -> ConsumerProperties
brokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

-- | Disables auto commit for the consumer
noAutoCommit :: ConsumerProperties
noAutoCommit =
  extraProps $ M.fromList [("enable.auto.commit", "false")]

-- | Disables auto offset store for the consumer
noAutoStore :: ConsumerProperties
noAutoStore =
  extraProps $ M.fromList [("enable.auto.offset.store", "false")]

-- | Consumer group id
groupId :: ConsumerGroupId -> ConsumerProperties
groupId (ConsumerGroupId cid) =
  extraProps $ M.fromList [("group.id", cid)]

clientId :: ClientId -> ConsumerProperties
clientId (ClientId cid) =
  extraProps $ M.fromList [("client.id", cid)]

setCallback :: (KafkaConf -> IO ()) -> ConsumerProperties
setCallback cb = mempty { cpCallbacks = [cb] }

-- | Sets the logging level.
-- Usually is used with 'debugOptions' to configure which logs are needed.
logLevel :: KafkaLogLevel -> ConsumerProperties
logLevel ll = mempty { cpLogLevel = Just ll }

-- | Sets the compression codec for the consumer.
compression :: KafkaCompressionCodec -> ConsumerProperties
compression c =
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

-- | Suppresses consumer disconnects logs.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive connection.max.idle.ms value.
suppressDisconnectLogs :: ConsumerProperties
suppressDisconnectLogs =
  extraProps $ M.fromList [("log.connection.close", "false")]

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map String String -> ConsumerProperties
extraProps m = mempty { cpProps = m }
{-# INLINE extraProps #-}

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProp :: String -> String -> ConsumerProperties
extraProp k v = mempty { cpProps = M.singleton k v }
{-# INLINE extraProp #-}

-- | Sets debug features for the consumer.
-- Usually is used with 'consumerLogLevel'.
debugOptions :: [KafkaDebug] -> ConsumerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = L.intercalate "," (kafkaDebugToString <$> d)
   in extraProps $ M.fromList [("debug", points)]

queuedMaxMessagesKBytes :: Int -> ConsumerProperties
queuedMaxMessagesKBytes kBytes =
  extraProp "queued.max.messages.kbytes" (show kBytes)
{-# INLINE queuedMaxMessagesKBytes #-}
