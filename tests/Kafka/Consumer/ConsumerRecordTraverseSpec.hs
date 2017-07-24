{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer.ConsumerRecordTraverseSpec
( spec
) where

import Data.Bifunctor
import Data.Bitraversable
import Kafka.Consumer.Types
import Kafka.Types
import Test.Hspec

testKey, testValue :: String
testKey   = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord String String
testRecord = ConsumerRecord
  { crTopic     = TopicName "some-topic"
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

liftValueM :: a -> Maybe (Either String a)
liftValueM = pure . pure

liftNothingM :: a -> Maybe (Either String a)
liftNothingM _ = Nothing

testError :: Either String a
testError = Left "test error"

liftErrorM :: a -> Maybe (Either String a)
liftErrorM _ = Just testError

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

  it "should traverse key (monadic)" $
    traverseFirstM liftValueM testRecord `shouldBe` Just (Right testRecord)

  it "should traverse value (monadic)" $
    traverseM liftValueM testRecord `shouldBe` Just (Right testRecord)

  it "should traverse KV (monadic)" $
    bitraverseM liftValueM liftValueM testRecord `shouldBe` Just (Right testRecord)

  it "should traverse and report error (monadic)" $ do
    traverseFirstM liftErrorM testRecord `shouldBe` Just testError
    traverseM liftErrorM testRecord `shouldBe` Just testError
    bitraverseM liftValueM liftErrorM testRecord `shouldBe` Just testError
    bitraverseM liftErrorM liftValueM testRecord `shouldBe` Just testError

  it "should traverse and report empty container (monadic)" $ do
    traverseFirstM liftNothingM testRecord `shouldBe` Nothing
    traverseM liftNothingM testRecord `shouldBe` Nothing
    bitraverseM liftValueM liftNothingM testRecord `shouldBe` Nothing
    bitraverseM liftNothingM liftValueM testRecord `shouldBe` Nothing
