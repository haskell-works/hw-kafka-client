module Kafka.Consumer.ConsumerProperties
where

--
import Data.Map (Map)
import Kafka.Types
import Kafka.Consumer.Types
import qualified Data.Map as M
import qualified Data.List as L

newtype ConsumerProperties = ConsumerProperties (Map String String)
  deriving (Show)

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

clientId :: ClientId -> ConsumerProperties
clientId (ClientId cid) =
  ConsumerProperties $ M.fromList [("client.id", cid)]

extraConsumerProps :: Map String String -> ConsumerProperties
extraConsumerProps = ConsumerProperties
