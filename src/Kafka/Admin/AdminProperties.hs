module Kafka.Admin.AdminProperties
where

import           Control.Monad (mplus)
import qualified Data.List     as L
import           Data.Map      (Map)
import qualified Data.Map      as M
import           Kafka.Types

-- | Properties to create 'KafkaProducer'.
data AdminProperties = AdminProperties
  { apKafkaProps :: Map String String
  , apLogLevel   :: Maybe KafkaLogLevel
  }

instance Semigroup AdminProperties where
  (AdminProperties k1 ll1) <> (AdminProperties k2 ll2) =
    AdminProperties (M.union k2 k1) (ll2 `mplus` ll1)
  {-# INLINE (<>) #-}

-- | /Right biased/ so we prefer newer properties over older ones.
instance Monoid AdminProperties where
  mempty = AdminProperties
    { apKafkaProps     = M.empty
    , apLogLevel       = Nothing
    }
  {-# INLINE mempty #-}
  mappend = (<>)
  {-# INLINE mappend #-}

brokersList :: [BrokerAddress] -> AdminProperties
brokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProps $ M.fromList [("bootstrap.servers", bs')]

-- | Any configuration options that are supported by /librdkafka/.
-- The full list can be found <https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md here>
extraProps :: Map String String -> AdminProperties
extraProps m = mempty { apKafkaProps = m }

-- | Suppresses producer disconnects logs.
--
-- It might be useful to turn this off when interacting with brokers
-- with an aggressive connection.max.idle.ms value.
suppressDisconnectLogs :: AdminProperties
suppressDisconnectLogs =
  extraProps $ M.fromList [("log.connection.close", "false")]
