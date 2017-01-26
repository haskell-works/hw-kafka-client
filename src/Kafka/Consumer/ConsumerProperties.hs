{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Kafka.Consumer.ConsumerProperties
where

--
import Data.Map (Map)
import Kafka.Types
import qualified Data.Map as M
import qualified Data.List as L

newtype Millis   = Millis Int deriving (Show, Eq, Ord, Num)
newtype ClientId = ClientId String deriving (Show, Eq, Ord)
data OffsetReset = Earliest | Latest deriving (Show, Eq)

newtype ConsumerProperties = ConsumerProperties (Map String String)
  deriving (Show)

newtype ConsumerGroupId = ConsumerGroupId String deriving (Show, Eq, Ord)

instance Monoid ConsumerProperties where
  mempty = ConsumerProperties M.empty
  mappend (ConsumerProperties m1) (ConsumerProperties m2) = ConsumerProperties (M.union m1 m2)

consumerBrokersList :: [BrokerAddress] -> ConsumerProperties
consumerBrokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in ConsumerProperties $ M.fromList [("bootstrap.servers", bs')]

groupId :: ConsumerGroupId -> ConsumerProperties
groupId (ConsumerGroupId cid) =
  ConsumerProperties $ M.fromList [("group.id", cid)]

offsetReset :: OffsetReset -> ConsumerProperties
offsetReset o =
  let o' = case o of
             Earliest -> "earliest"
             Latest   -> "latest"
   in ConsumerProperties $ M.fromList [("auto.offset.reset", o')]

noAutoCommit :: ConsumerProperties
noAutoCommit =
  ConsumerProperties $ M.fromList [("enable.auto.commit", "false")]

autoCommit :: Millis -> ConsumerProperties
autoCommit (Millis ms) = ConsumerProperties $
  M.fromList
    [ ("enable.auto.commit", "true")
    , ("auto.commit.interval.ms", show ms)
    ]

clientId :: ClientId -> ConsumerProperties
clientId (ClientId cid) =
  ConsumerProperties $ M.fromList [("client.id", cid)]

extraProps :: Map String String -> ConsumerProperties
extraProps = ConsumerProperties
