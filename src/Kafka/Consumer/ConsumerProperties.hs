{-# LANGUAGE OverloadedStrings #-}

module Kafka.Consumer.ConsumerProperties
( ConsumerProperties(..)
, brokersList
, noAutoCommit
, noAutoOffsetStore
, groupId
, clientId
, setCallback
, logLevel
, compression
, suppressDisconnectLogs
, extraProps
, extraProp
, debugOptions
, queuedMaxMessagesKBytes
, userPolls
, module X
)
where

import           Control.Monad        (MonadPlus(mplus))
import           Data.Map             (Map)
import           Data.Monoid          (Any)
import qualified Data.Map             as M
import           Data.Semigroup       as Sem
import           Data.Text            (Text)
import qualified Data.Text            as Text
import           Kafka.Consumer.Types (ConsumerGroupId(..))
import           Kafka.Internal.Setup (KafkaConf(..))
import           Kafka.Types          (KafkaDebug(..), KafkaCompressionCodec(..), KafkaLogLevel(..), ClientId(..), BrokerAddress(..), kafkaDebugToText, kafkaCompressionCodecToText)

import Kafka.Consumer.Callbacks as X

-- | Properties to create 'KafkaConsumer'.
data ConsumerProperties = ConsumerProperties
  { cpProps     :: Map Text Text
  , cpLogLevel  :: Maybe KafkaLogLevel
  , cpCallbacks :: [KafkaConf -> IO ()]
  , cpUserPolls :: Any
  }

instance Sem.Semigroup ConsumerProperties where
  (ConsumerProperties m1 ll1 cb1 cup1) <> (ConsumerProperties m2 ll2 cb2 cup2) =
    ConsumerProperties (M.union m2 m1) (ll2 `mplus` ll1) (cb1 `mplus` cb2) (cup1 <> cup2)
  {-# INLINE (<>) #-}

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid ConsumerProperties where
  mempty = ConsumerProperties
    { cpProps          = M.empty
    , cpLogLevel       = Nothing
    , cpCallbacks      = []
    , cpUserPolls      = Any False
    }
  {-# INLINE mempty #-}
  mappend = (Sem.<>)
  {-# INLINE mappend #-}

brokersList :: [BrokerAddress] -> ConsumerProperties
brokersList bs =
  let bs' = Text.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

-- | Disables auto commit for the consumer
noAutoCommit :: ConsumerProperties
noAutoCommit =
  extraProps $ M.fromList [("enable.auto.commit", "false")]

-- | Disables auto offset store for the consumer
noAutoOffsetStore :: ConsumerProperties
noAutoOffsetStore =
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
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToText c)

-- | Suppresses consumer disconnects logs.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive connection.max.idle.ms value.
suppressDisconnectLogs :: ConsumerProperties
suppressDisconnectLogs =
  extraProps $ M.fromList [("log.connection.close", "false")]

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map Text Text -> ConsumerProperties
extraProps m = mempty { cpProps = m }
{-# INLINE extraProps #-}

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProp :: Text -> Text -> ConsumerProperties
extraProp k v = mempty { cpProps = M.singleton k v }
{-# INLINE extraProp #-}

-- | Sets debug features for the consumer.
-- Usually is used with 'consumerLogLevel'.
debugOptions :: [KafkaDebug] -> ConsumerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = Text.intercalate "," (kafkaDebugToText <$> d)
   in extraProps $ M.fromList [("debug", points)]

queuedMaxMessagesKBytes :: Int -> ConsumerProperties
queuedMaxMessagesKBytes kBytes =
  extraProp "queued.max.messages.kbytes" (Text.pack $ show kBytes)
{-# INLINE queuedMaxMessagesKBytes #-}

-- | The user will poll the consumer frequently to handle both new
-- messages and rebalance events.
--
-- By default hw-kafka-client handles polling rebalance events for you
-- in a background thread, with this property set you can simplify
-- hw-kafka-client's footprint and have full control over when polling
-- happens at the cost of having to manage this yourself.
userPolls :: ConsumerProperties
userPolls = mempty { cpUserPolls = Any True }
