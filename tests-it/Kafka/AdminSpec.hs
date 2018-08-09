{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.AdminSpec
where

import           Control.Monad       (forM, forM_)
import           Control.Monad.Loops
import qualified Data.ByteString     as BS
import           Data.Either
import           Data.Monoid         ((<>))

import Kafka.Admin    as A
import Kafka.Consumer as C
import Kafka.Metadata as M
import Kafka.Producer as P

import Control.Concurrent
import Data.Map           as M

import Kafka.TestEnv
import Test.Hspec

{-# ANN module ("HLint: ignore Redundant do"  :: String) #-}

spec :: Spec
spec = do
  describe "Kafka.AdminSpec" $ do
    let adminTopic = TopicName "admin-1"

    specWithAdmin "Create topic spec" $ do

      it "1. Creates one topic" $ \admin ->  do
        let newTopic = NewTopic adminTopic (PartitionsCount 3) (ReplicationFactor 1) M.empty
        res <- createTopics admin [newTopic]
        res `shouldSatisfy` all isRight

      it "2. Delete that newly created topic" $ \admin -> do
        threadDelay (1000*1000)
        res <- deleteTopics admin [adminTopic]
        res `shouldSatisfy` all isRight


