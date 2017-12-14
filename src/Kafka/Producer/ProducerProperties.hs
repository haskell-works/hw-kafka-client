module Kafka.Producer.ProducerProperties
( module Kafka.Producer.ProducerProperties
, module Kafka.Producer.Callbacks
)
where

import           Control.Monad
import qualified Data.List                as L
import           Data.Map                 (Map)
import qualified Data.Map                 as M
import           Kafka.Internal.Setup
import           Kafka.Producer.Callbacks
import           Kafka.Types

-- | Properties to create 'KafkaProducer'.
data ProducerProperties = ProducerProperties
  { ppKafkaProps :: Map String String
  , ppTopicProps :: Map String String
  , ppLogLevel   :: Maybe KafkaLogLevel
  , ppCallbacks  :: [KafkaConf -> IO ()]
  }

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid ProducerProperties where
  mempty = ProducerProperties
    { ppKafkaProps     = M.empty
    , ppTopicProps     = M.empty
    , ppLogLevel       = Nothing
    , ppCallbacks      = []
    }
  {-# INLINE mempty #-}
  mappend (ProducerProperties k1 t1 ll1 cb1) (ProducerProperties k2 t2 ll2 cb2) =
    ProducerProperties (M.union k2 k1) (M.union t2 t1) (ll2 `mplus` ll1) (cb1 `mplus` cb2)
  {-# INLINE mappend #-}

brokersList :: [BrokerAddress] -> ProducerProperties
brokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

setCallback :: (KafkaConf -> IO ()) -> ProducerProperties
setCallback cb = mempty { ppCallbacks = [cb] }

-- | Sets the logging level.
-- Usually is used with 'debugOptions' to configure which logs are needed.
logLevel :: KafkaLogLevel -> ProducerProperties
logLevel ll = mempty { ppLogLevel = Just ll }

compression :: KafkaCompressionCodec -> ProducerProperties
compression c =
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

topicCompression :: KafkaCompressionCodec -> ProducerProperties
topicCompression c =
  extraTopicProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

sendTimeout :: Timeout -> ProducerProperties
sendTimeout (Timeout t) =
  extraTopicProps $ M.singleton "message.timeout.ms" (show t)

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map String String -> ProducerProperties
extraProps m = mempty { ppKafkaProps = m }

-- | Suppresses producer disconnects logs.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive connection.max.idle.ms value.
suppressDisconnectLogs :: ProducerProperties
suppressDisconnectLogs =
  extraProps $ M.fromList [("log.connection.close", "false")]

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraTopicProps :: Map String String -> ProducerProperties
extraTopicProps m = mempty { ppTopicProps = m }

-- | Sets debug features for the producer
-- Usually is used with 'logLevel'.
debugOptions :: [KafkaDebug] -> ProducerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = L.intercalate "," (kafkaDebugToString <$> d)
   in extraProps $ M.fromList [("debug", points)]
