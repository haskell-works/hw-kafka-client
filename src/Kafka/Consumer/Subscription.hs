{-# LANGUAGE OverloadedStrings #-}

module Kafka.Consumer.Subscription
( Subscription(..)
, topics
, offsetReset
, extraSubscriptionProps
)
where

import qualified Data.Text            as Text
import           Data.Text            (Text)
import           Data.Map             (Map)
import qualified Data.Map             as M
import           Data.Semigroup       as Sem
import           Kafka.Consumer.Types (OffsetReset(..))
import           Kafka.Types          (TopicName(..), Millis(..))
import           Data.Set             (Set)
import qualified Data.Set             as Set

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

topics :: [TopicName] -> Subscription
topics ts = Subscription (Set.fromList ts) M.empty

offsetReset :: OffsetReset -> Subscription
offsetReset o =
  let o' = case o of
             Earliest -> "earliest"
             Latest   -> "latest"
   in Subscription (Set.empty) (M.fromList [("auto.offset.reset", o')])

extraSubscriptionProps :: Map Text Text -> Subscription
extraSubscriptionProps = Subscription (Set.empty)
