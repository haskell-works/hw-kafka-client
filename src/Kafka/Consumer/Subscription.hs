{-# LANGUAGE OverloadedStrings #-}

module Kafka.Consumer.Subscription
( Subscription(..)
, topics
, offsetReset
, extraSubscriptionProps
)
where

import           Data.Map             (Map)
import qualified Data.Map             as M
import           Data.Semigroup       as Sem
import           Data.Set             (Set)
import qualified Data.Set             as Set
import           Data.Text            (Text)
import           Kafka.Consumer.Types (OffsetReset (..))
import           Kafka.Types          (TopicName (..))

-- | A consumer subscription to a topic.
--
-- ==== __Examples__
--
-- Typically you don't call the constructor directly, but combine settings:
--
-- @
-- consumerSub :: 'Subscription'
-- consumerSub = 'topics' ['TopicName' "kafka-client-example-topic"]
--         <> 'offsetReset' 'Earliest'
--         <> 'extraSubscriptionProps' (fromList [("prop1", "value 1"), ("prop2", "value 2")])
-- @
data Subscription = Subscription (Set TopicName) (Map Text Text)

instance Sem.Semigroup Subscription where
  (Subscription ts1 m1) <> (Subscription ts2 m2) =
    let ts' = Set.union ts1 ts2
        ps' = M.union m1 m2
     in Subscription ts' ps'
  {-# INLINE (<>) #-}

instance Monoid Subscription where
  mempty = Subscription Set.empty M.empty
  {-# INLINE mempty #-}
  mappend = (Sem.<>)
  {-# INLINE mappend #-}

-- | Build a subscription by giving the list of topic names only
topics :: [TopicName] -> Subscription
topics ts = Subscription (Set.fromList ts) M.empty

-- | Build a subscription by giving the offset reset parameter only
offsetReset :: OffsetReset -> Subscription
offsetReset o =
  let o' = case o of
             Earliest -> "earliest"
             Latest   -> "latest"
   in Subscription Set.empty (M.fromList [("auto.offset.reset", o')])

-- | Build a subscription by giving extra properties only
extraSubscriptionProps :: Map Text Text -> Subscription
extraSubscriptionProps = Subscription Set.empty
