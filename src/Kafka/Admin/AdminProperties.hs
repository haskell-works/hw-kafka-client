{-# LANGUAGE OverloadedStrings #-}

module Kafka.Admin.AdminProperties where

import Data.Map
import qualified Data.Map as M
import Data.Text

import Kafka.Types

newtype AdminProperties = AdminProperties {
  adminProps :: Map Text Text
}

instance Semigroup AdminProperties where
  ( AdminProperties props1 ) <> ( AdminProperties props2 ) =
    AdminProperties ( props2 `union` props1 )
  {-# INLINE (<>) #-}

instance Monoid AdminProperties where
  mempty = AdminProperties {
    adminProps     = M.empty
  }
  {-# INLINE mempty #-}
  mappend = (<>)
  {-# INLINE mappend #-}

brokers :: [BrokerAddress] -> AdminProperties
brokers b =
  let b' = intercalate "," ((\( BrokerAddress i ) -> i ) <$> b )
  in  extraProps $ fromList [("bootstrap.servers",  b')]

clientId :: ClientId -> AdminProperties
clientId (ClientId cid) =
  extraProps $ M.fromList [("client.id", cid)]

timeOut :: Timeout -> AdminProperties
timeOut (Timeout to) =
  let to' = ( pack $ show to )
  in extraProps $ fromList [("request.timeout.ms", to')]

extraProps :: Map Text Text -> AdminProperties
extraProps m = mempty { adminProps = m }
