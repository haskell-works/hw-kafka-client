{-# LANGUAGE OverloadedStrings #-}

module Kafka.Consumer.ConsumerRecordTraverseSpec
( spec
) where

import Data.Bifunctor
import Data.Bitraversable
import Data.Text
import Kafka.Consumer.Types
import Kafka.Types
import Test.Hspec

testKey, testValue :: Text
testKey   = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord Text Text
testRecord = ConsumerRecord
  { crTopic     = "some-topic"
  , crPartition = PartitionId 0
  , crOffset    = Offset 5
  , crTimestamp = NoTimestamp
  , crKey       = testKey
  , crValue     = testValue
  }

liftValue :: a -> Maybe a
liftValue = Just

liftNothing :: a -> Maybe a
liftNothing _ = Nothing

spec :: Spec
spec = describe "Kafka.Consumer.ConsumerRecordTraverseSpec" $ do
  it "should sequence" $ do
    sequence  (liftValue <$> testRecord) `shouldBe` Just testRecord
    sequenceA (liftValue <$> testRecord) `shouldBe` Just testRecord
    sequence (liftNothing <$> testRecord) `shouldBe` Nothing

  it "should traverse" $ do
    traverse liftValue testRecord `shouldBe` Just testRecord
    traverse liftNothing testRecord `shouldBe` Nothing

  it "should bisequence" $ do
    bisequence (bimap liftValue liftValue testRecord) `shouldBe` Just testRecord
    bisequence (bimap liftNothing liftValue testRecord) `shouldBe` Nothing
    bisequence (bimap liftValue liftNothing testRecord) `shouldBe` Nothing
    bisequenceA (bimap liftValue liftValue testRecord) `shouldBe` Just testRecord
    bisequenceA (bimap liftNothing liftValue testRecord) `shouldBe` Nothing
    bisequenceA (bimap liftValue liftNothing testRecord) `shouldBe` Nothing

  it "should bitraverse" $ do
    bitraverse liftValue liftValue testRecord `shouldBe` Just testRecord
    bitraverse liftNothing liftValue testRecord `shouldBe` Nothing
    bitraverse liftValue liftNothing testRecord `shouldBe` Nothing

