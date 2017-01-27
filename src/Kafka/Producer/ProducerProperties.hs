module Kafka.Producer.ProducerProperties
where

--
import Data.Map (Map)
import Kafka.Types
import qualified Data.Map as M
import qualified Data.List as L

data ProducerProperties = ProducerProperties (Map String String) (Map String String)
  deriving (Show)

instance Monoid ProducerProperties where
  mempty = ProducerProperties M.empty M.empty
  mappend (ProducerProperties k1 t1) (ProducerProperties k2 t2) =
    ProducerProperties (M.union k1 k2) (M.union t1 t2)

producerBrokersList :: [BrokerAddress] -> ProducerProperties
producerBrokersList bs =
  let bs' = L.intercalate "," ((\(BrokerAddress x) -> x) <$> bs)
   in extraProducerProps $ M.fromList [("bootstrap.servers", bs')]

extraProducerProps :: Map String String -> ProducerProperties
extraProducerProps m = ProducerProperties m M.empty

extraProducerTopicProps :: Map String String -> ProducerProperties
extraProducerTopicProps = ProducerProperties M.empty
