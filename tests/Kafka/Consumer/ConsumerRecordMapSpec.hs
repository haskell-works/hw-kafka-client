{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer.ConsumerRecordMapSpec
( spec
) where

import Data.Bitraversable
import Data.Text
import Kafka.Consumer.Types
import Kafka.Types
import Test.Hspec

testKey, testValue :: Text
testKey   = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord (Maybe Text) (Maybe Text)
testRecord = ConsumerRecord
  { crTopic     = TopicName "some-topic"
  , crPartition = PartitionId 0
  , crOffset    = Offset 5
  , crTimestamp = NoTimestamp
  , crKey       = Just testKey
  , crValue     = Just testValue
  }

spec :: Spec
spec = describe "Kafka.Consumer.ConsumerRecordSpec" $ do
    it "should exract key" $
      bitraverse id pure testRecord `shouldBe` Just (crMapKey (const testKey) testRecord)

    it "should extract value" $
      sequence testRecord `shouldBe` Just (crMapValue (const testValue) testRecord)

    it "should extract both key and value" $
      bisequence testRecord `shouldBe` Just (crMapKV (const testKey) (const testValue) testRecord)
