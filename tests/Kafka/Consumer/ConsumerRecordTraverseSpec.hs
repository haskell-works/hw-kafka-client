{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer.ConsumerRecordTraverseSpec
( spec
) where

import Kafka.Types
import Kafka.Consumer.Types
import Test.Hspec

testKey, testValue :: String
testKey   = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord String String
testRecord = ConsumerRecord
  { messageTopic     = TopicName "some-topic"
  , messagePartition = PartitionId 0
  , messageOffset    = Offset 5
  , messageKey       = testKey
  , messagePayload   = testValue
  }

liftValue :: a -> Maybe (Either String a)
liftValue = pure . pure

liftNothing :: a -> Maybe (Either String a)
liftNothing _ = Nothing

testError :: Either String a
testError = Left "test error"
liftError :: a -> Maybe (Either String a)
liftError _ = Just testError

spec :: Spec
spec = describe "Kafka.Consumer.ConsumerRecordTraverseSpec" $ do
  it "should traverse key (monadic)" $
    crTraverseKeyM liftValue testRecord `shouldBe` Just (Right testRecord)

  it "should traverse value (monadic)" $
    crTraverseValueM liftValue testRecord `shouldBe` Just (Right testRecord)

  it "should traverse KV (monadic)" $
    crTraverseKVM liftValue liftValue testRecord `shouldBe` Just (Right testRecord)

  it "should traverse and report error (monadic)" $ do
    crTraverseKeyM liftError testRecord `shouldBe` Just testError
    crTraverseValueM liftError testRecord `shouldBe` Just testError
    crTraverseKVM liftValue liftError testRecord `shouldBe` Just testError
    crTraverseKVM liftError liftValue testRecord `shouldBe` Just testError

  it "should traverse and report empty container (monadic)" $ do
    crTraverseKeyM liftNothing testRecord `shouldBe` Nothing
    crTraverseValueM liftNothing testRecord `shouldBe` Nothing
    crTraverseKVM liftValue liftNothing testRecord `shouldBe` Nothing
    crTraverseKVM liftNothing liftValue testRecord `shouldBe` Nothing
