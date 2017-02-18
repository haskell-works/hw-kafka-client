module Kafka.Producer.ProducerProperties
where

import Control.Monad
import Data.Map (Map)
import Kafka.Types
import qualified Data.Map as M
import qualified Data.List as L

-- | Properties to create 'KafkaProducer'.
data ProducerProperties = ProducerProperties
  { ppKafkaProps :: Map String String
  , ppTopicProps :: Map String String
  , ppLogLevel   :: Maybe KafkaLogLevel
  } deriving (Show)

instance Monoid ProducerProperties where
  mempty = ProducerProperties M.empty M.empty Nothing
  mappend (ProducerProperties k1 t1 ll1) (ProducerProperties k2 t2 ll2) =
    ProducerProperties (M.union k1 k2) (M.union t1 t2) (ll2 `mplus` ll1)

producerBrokersList :: [BrokerAddress] -> ProducerProperties
producerBrokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProducerProps $ M.fromList [("bootstrap.servers", bs')]

-- | Sets the logging level.
-- Usually is used with 'producerDebug' to configure which logs are needed.
producerLogLevel :: KafkaLogLevel -> ProducerProperties
producerLogLevel ll = ProducerProperties M.empty M.empty (Just ll)

producerCompression :: KafkaCompressionCodec -> ProducerProperties
producerCompression c =
  extraProducerProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

producerTopicCompression :: KafkaCompressionCodec -> ProducerProperties
producerTopicCompression c =
  extraProducerTopicProps $ M.singleton "compression.codec" (kafkaCompressionCodecToString c)

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProducerProps :: Map String String -> ProducerProperties
extraProducerProps m = ProducerProperties m M.empty Nothing

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProducerTopicProps :: Map String String -> ProducerProperties
extraProducerTopicProps m = ProducerProperties M.empty m Nothing

-- | Sets debug features for the producer
-- Usually is used with 'producerLogLevel'.
producerDebug :: [KafkaDebug] -> ProducerProperties
producerDebug [] = extraProducerProps M.empty
producerDebug d =
  let points = L.intercalate "," (kafkaDebugToString <$> d)
   in extraProducerProps $ M.fromList [("debug", points)]
