module Kafka.Producer.ProducerProperties
where

--
import Control.Monad
import Data.Map (Map)
import Kafka.Types
import qualified Data.Map as M
import qualified Data.List as L

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

producerLogLevel :: KafkaLogLevel -> ProducerProperties
producerLogLevel ll = ProducerProperties M.empty M.empty (Just ll)

extraProducerProps :: Map String String -> ProducerProperties
extraProducerProps m = ProducerProperties m M.empty Nothing

extraProducerTopicProps :: Map String String -> ProducerProperties
extraProducerTopicProps m = ProducerProperties M.empty m Nothing
