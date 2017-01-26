module Kafka.Consumer.Subscription
where

--
import Data.Map (Map)
import qualified Data.Map as M
import qualified Data.List as L
import Kafka.Types
import Kafka.Consumer.Types

data Subscription = Subscription [TopicName] (Map String String)

instance Monoid Subscription where
  mempty = Subscription [] M.empty
  mappend (Subscription ts1 m1) (Subscription ts2 m2) =
    let ts' = L.nub $ L.union ts1 ts2
        ps' = M.union m1 m2
     in Subscription ts' ps'

topics :: [TopicName] -> Subscription
topics ts = Subscription (L.nub ts) M.empty

offsetReset :: OffsetReset -> Subscription
offsetReset o =
  let o' = case o of
             Earliest -> "earliest"
             Latest   -> "latest"
   in Subscription [] (M.fromList [("auto.offset.reset", o')])

noAutoCommit :: Subscription
noAutoCommit =
  Subscription [] (M.fromList [("enable.auto.commit", "false")])

autoCommit :: Millis -> Subscription
autoCommit (Millis ms) = Subscription [] $
  M.fromList
    [ ("enable.auto.commit", "true")
    , ("auto.commit.interval.ms", show ms)
    ]

extraSubscriptionProps :: Map String String -> Subscription
extraSubscriptionProps = Subscription []
