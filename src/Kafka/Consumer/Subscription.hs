module Kafka.Consumer.Subscription
where

import qualified Data.List            as L
import           Data.Map             (Map)
import qualified Data.Map             as M
import           Data.Semigroup       as Sem
import           Kafka.Consumer.Types
import           Kafka.Types

data Subscription = Subscription [TopicName] (Map String String)

instance Sem.Semigroup Subscription where
  (Subscription ts1 m1) <> (Subscription ts2 m2) =
    let ts' = L.nub $ L.union ts1 ts2
        ps' = M.union m1 m2
     in Subscription ts' ps'
  {-# INLINE (<>) #-}

instance Monoid Subscription where
  mempty = Subscription [] M.empty
  {-# INLINE mempty #-}
  mappend = (Sem.<>)
  {-# INLINE mappend #-}

topics :: [TopicName] -> Subscription
topics ts = Subscription (L.nub ts) M.empty

offsetReset :: OffsetReset -> Subscription
offsetReset o =
  let o' = case o of
             Earliest -> "earliest"
             Latest   -> "latest"
   in Subscription [] (M.fromList [("auto.offset.reset", o')])

autoCommit :: Millis -> Subscription
autoCommit (Millis ms) = Subscription [] $
  M.fromList
    [ ("enable.auto.commit", "true")
    , ("auto.commit.interval.ms", show ms)
    ]

extraSubscriptionProps :: Map String String -> Subscription
extraSubscriptionProps = Subscription []
