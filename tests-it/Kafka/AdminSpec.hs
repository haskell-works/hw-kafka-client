{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.AdminSpec
where

import           Control.Monad       (forM, forM_)
import           Control.Monad.Loops
import qualified Data.ByteString     as BS
import           Data.Either
import           Data.Map
import           Data.Monoid         ((<>))

import Kafka.Admin    as A
import Kafka.Consumer as C
import Kafka.Metadata as M
import Kafka.Producer as P

import Control.Concurrent

import Kafka.TestEnv
import Test.Hspec

{-# ANN module ("HLint: ignore Redundant do"  :: String) #-}

spec :: Spec
spec = do
    describe "Kafka.AdminSpec" $ do
        specWithAdmin "Create topic spec" $ do
            it "1. Creates one topic" $ \admin ->  do
                res <- createTopics admin [(TopicName "admin-1", PartitionsCount 3, ReplicationFactor 1)]
                -- threadDelay (1000*10000)
                res `shouldSatisfy` all isRight

