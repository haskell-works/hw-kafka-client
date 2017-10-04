module Kafka.Producer.ProducerProperties
( module Kafka.Producer.ProducerProperties
, module Kafka.Callbacks
)
where

import           Control.Monad
import qualified Data.List       as L
import           Data.Map        (Map)
import qualified Data.Map        as M
import           Data.Semigroup
import           Kafka.Callbacks
import           Kafka.Types

-- | Properties to create 'KafkaProducer'.
data ProducerProperties = ProducerProperties
  { ppKafkaProps :: Map String String
  , ppTopicProps :: Map String String
  , ppLogLevel   :: Maybe KafkaLogLevel
  , callbacks    :: [KafkaConf -> IO ()]
  }
  
-- | /Right biased/ so we prefer newer properties over older ones.
instance Semigroup ProducerProperties where
  (<>) (ProducerProperties k1 t1 ll1 cb1) (ProducerProperties k2 t2 ll2 cb2) =
    ProducerProperties (M.union k2 k1) (M.union t2 t1) (ll2 `mplus` ll1) (cb2 `mplus` cb1)
    
instance Monoid ProducerProperties where
  mempty = ProducerProperties M.empty M.empty Nothing []
  mappend = (<>)

brokersList :: [BrokerAddress] -> ProducerProperties
brokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

setCallback :: (KafkaConf -> IO ()) -> ProducerProperties
setCallback cb = ProducerProperties M.empty M.empty Nothing [cb]

-- | Sets the logging level.
-- Usually is used with 'debugOptions' to configure which logs are needed.
logLevel :: KafkaLogLevel -> ProducerProperties
logLevel ll = ProducerProperties M.empty M.empty (Just ll) []

compression :: KafkaCompressionCodec -> ProducerProperties
compression c =
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

topicCompression :: KafkaCompressionCodec -> ProducerProperties
topicCompression c =
  extraTopicProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map String String -> ProducerProperties
extraProps m = ProducerProperties m M.empty Nothing []

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
extraTopicProps m = ProducerProperties M.empty m Nothing []

-- | Sets debug features for the producer
-- Usually is used with 'logLevel'.
debugOptions :: [KafkaDebug] -> ProducerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = L.intercalate "," (kafkaDebugToString <$> d)
   in extraProps $ M.fromList [("debug", points)]
