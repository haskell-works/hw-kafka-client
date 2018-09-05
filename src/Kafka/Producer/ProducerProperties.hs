{-# LANGUAGE OverloadedStrings #-}

module Kafka.Producer.ProducerProperties
( ProducerProperties(..)
, brokersList
, setCallback
, logLevel
, compression
, topicCompression
, sendTimeout
, extraProps
, suppressDisconnectLogs
, extraTopicProps
, debugOptions
, module Kafka.Producer.Callbacks
)
where

import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Control.Monad            (MonadPlus(mplus))
import           Data.Map                 (Map)
import qualified Data.Map                 as M
import           Data.Semigroup           as Sem
import           Kafka.Internal.Setup     (KafkaConf(..))
import           Kafka.Types              (KafkaDebug(..), Timeout(..), KafkaCompressionCodec(..), KafkaLogLevel(..), BrokerAddress(..), kafkaDebugToText, kafkaCompressionCodecToText)  

import           Kafka.Producer.Callbacks

-- | Properties to create 'KafkaProducer'.
data ProducerProperties = ProducerProperties
  { ppKafkaProps :: Map Text Text
  , ppTopicProps :: Map Text Text
  , ppLogLevel   :: Maybe KafkaLogLevel
  , ppCallbacks  :: [KafkaConf -> IO ()]
  }

instance Sem.Semigroup ProducerProperties where
  (ProducerProperties k1 t1 ll1 cb1) <> (ProducerProperties k2 t2 ll2 cb2) =
    ProducerProperties (M.union k2 k1) (M.union t2 t1) (ll2 `mplus` ll1) (cb1 `mplus` cb2)
  {-# INLINE (<>) #-}

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid ProducerProperties where
  mempty = ProducerProperties
    { ppKafkaProps     = M.empty
    , ppTopicProps     = M.empty
    , ppLogLevel       = Nothing
    , ppCallbacks      = []
    }
  {-# INLINE mempty #-}
  mappend = (Sem.<>)
  {-# INLINE mappend #-}

brokersList :: [BrokerAddress] -> ProducerProperties
brokersList bs =
  let bs' = Text.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

setCallback :: (KafkaConf -> IO ()) -> ProducerProperties
setCallback cb = mempty { ppCallbacks = [cb] }

-- | Sets the logging level.
-- Usually is used with 'debugOptions' to configure which logs are needed.
logLevel :: KafkaLogLevel -> ProducerProperties
logLevel ll = mempty { ppLogLevel = Just ll }

compression :: KafkaCompressionCodec -> ProducerProperties
compression c =
  extraProps $ M.singleton "compression.codec" (kafkaCompressionCodecToText c)

topicCompression :: KafkaCompressionCodec -> ProducerProperties
topicCompression c =
  extraTopicProps $ M.singleton "compression.codec" (kafkaCompressionCodecToText c)

sendTimeout :: Timeout -> ProducerProperties
sendTimeout (Timeout t) =
  extraTopicProps $ M.singleton "message.timeout.ms" (Text.pack $ show t)

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map Text Text -> ProducerProperties
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
extraTopicProps :: Map Text Text -> ProducerProperties
extraTopicProps m = mempty { ppTopicProps = m }

-- | Sets debug features for the producer
-- Usually is used with 'logLevel'.
debugOptions :: [KafkaDebug] -> ProducerProperties
debugOptions [] = extraProps M.empty
debugOptions d =
  let points = Text.intercalate "," (kafkaDebugToText <$> d)
   in extraProps $ M.fromList [("debug", points)]
