module Kafka.Consumer.ConsumerProperties
where

--
import Control.Monad
import Data.Map (Map)
import Kafka.Types
import Kafka.Consumer.Types
import qualified Data.Map as M
import qualified Data.List as L

-- | Properties to create 'KafkaConsumer'.
data ConsumerProperties = ConsumerProperties
  { cpProps             :: Map String String
  , cpRebalanceCallback :: Maybe ReballanceCallback
  , cpOffsetsCallback   :: Maybe OffsetsCommitCallback
  , cpLogLevel          :: Maybe KafkaLogLevel
  }

instance Monoid ConsumerProperties where
  mempty = ConsumerProperties M.empty Nothing Nothing Nothing
  mappend (ConsumerProperties m1 rb1 oc1 ll1) (ConsumerProperties m2 rb2 oc2 ll2) =
    ConsumerProperties (M.union m1 m2) (rb2 `mplus` rb1) (oc2 `mplus` oc1) (ll2 `mplus` ll1)

consumerBrokersList :: [BrokerAddress] -> ConsumerProperties
consumerBrokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraConsumerProps $ M.fromList [("bootstrap.servers", bs')]

-- | Disables auto commit for the consumer
noAutoCommit :: ConsumerProperties
noAutoCommit =
  extraConsumerProps $ M.fromList [("enable.auto.commit", "false")]

-- | Consumer group id
groupId :: ConsumerGroupId -> ConsumerProperties
groupId (ConsumerGroupId cid) =
  extraConsumerProps $ M.fromList [("group.id", cid)]

clientId :: ClientId -> ConsumerProperties
clientId (ClientId cid) =
  extraConsumerProps $ M.fromList [("client.id", cid)]

-- | Sets a callback that is called when rebalance is needed.
--
-- Callback implementations suppose to watch for 'KafkaResponseError' 'RdKafkaRespErrAssignPartitions' and
-- for 'KafkaResponseError' 'RdKafkaRespErrRevokePartitions'. Other error codes are not expected and would indicate
-- something really bad happening in a system, or bugs in @librdkafka@ itself.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
--     * When 'RdKafkaRespErrAssignPartitions' happens 'assign' should be called with all the partitions it was called with.
--       It is OK to alter partitions offsets before calling 'assign'.
--
--     * When 'RdKafkaRespErrRevokePartitions' happens 'assign' should be called with an empty list of partitions.
reballanceCallback :: ReballanceCallback -> ConsumerProperties
reballanceCallback cb = ConsumerProperties M.empty (Just cb) Nothing Nothing

-- | Sets offset commit callback for use with consumer groups.
--
-- The results of automatic or manual offset commits will be scheduled
-- for this callback and is served by `pollMessage`.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
-- If no partitions had valid offsets to commit this callback will be called
-- with `KafkaError` == `KafkaResponseError` `RdKafkaRespErrNoOffset` which is not to be considered
-- an error.
offsetsCommitCallback :: OffsetsCommitCallback -> ConsumerProperties
offsetsCommitCallback cb = ConsumerProperties M.empty Nothing (Just cb) Nothing

-- | Sets the logging level.
-- Usually is used with 'consumerDebug' to configure which logs are needed.
consumerLogLevel :: KafkaLogLevel -> ConsumerProperties
consumerLogLevel ll = ConsumerProperties M.empty Nothing Nothing (Just ll)

-- | Sets the compression codec for the consumer.
consumerCompression :: KafkaCompressionCodec -> ConsumerProperties
consumerCompression c =
  extraConsumerProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

-- | Suppresses consumer disconnects logs.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive connection.max.idle.ms value.
consumerSuppressDisconnectLogs :: ConsumerProperties
consumerSuppressDisconnectLogs =
  extraConsumerProps $ M.fromList [("log.connection.close", "false")]

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraConsumerProps :: Map String String -> ConsumerProperties
extraConsumerProps m = ConsumerProperties m Nothing Nothing Nothing
{-# INLINE extraConsumerProps #-}

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraConsumerProp :: String -> String -> ConsumerProperties
extraConsumerProp k v = ConsumerProperties (M.singleton k v) Nothing Nothing Nothing
{-# INLINE extraConsumerProp #-}

-- | Sets debug features for the consumer.
-- Usually is used with 'consumerLogLevel'.
consumerDebug :: [KafkaDebug] -> ConsumerProperties
consumerDebug [] = extraConsumerProps M.empty
consumerDebug d =
  let points = L.intercalate "," (kafkaDebugToString <$> d)
   in extraConsumerProps $ M.fromList [("debug", points)]

consumerQueuedMaxMessagesKBytes :: Int -> ConsumerProperties
consumerQueuedMaxMessagesKBytes kBytes =
  extraConsumerProp "queued.max.messages.kbytes" (show kBytes)
{-# INLINE consumerQueuedMaxMessagesKBytes #-}
