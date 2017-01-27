module Kafka.Producer.ProducerProperties
where

--
import Data.Map (Map)
import Kafka.Types
import qualified Data.Map as M
import qualified Data.List as L

newtype ProducerProperties = ProducerProperties (Map String String)
  deriving (Show)

instance Monoid ProducerProperties where
  mempty = ProducerProperties M.empty
  mappend (ProducerProperties m1) (ProducerProperties m2) = ProducerProperties (M.union m1 m2)

producerBrokersList :: [BrokerAddress] -> ProducerProperties
producerBrokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in ProducerProperties $ M.fromList [("bootstrap.servers", bs')]

extraProducerProps :: Map String String -> ProducerProperties
extraProducerProps = ProducerProperties
