{-# LANGUAGE OverloadedStrings #-}
module Kafka.Consumer.ConsumerRecordMapSpec
( spec
) where

import Kafka.Types
import Kafka.Consumer.Types
import Test.Hspec

testKey, testValue :: String
testKey   = "some-key"
testValue = "some-value"

testRecord :: ConsumerRecord (Maybe String) (Maybe String)
testRecord = ConsumerRecord
  { crTopic     = TopicName "some-topic"
  , crPartition = PartitionId 0
  , crOffset    = Offset 5
  , crKey       = Just testKey
  , crValue     = Just testValue
  }

spec :: Spec
spec = describe "Kafka.Consumer.ConsumerRecordSpec" $ do
    it "should exract key" $
      crSequenceKey testRecord `shouldBe` Just (crMapKey (const testKey) testRecord)

    it "should extract value" $
      crSequenceValue testRecord `shouldBe` Just (crMapValue (const testValue) testRecord)

    it "should extract both key and value" $
      crSequenceKV testRecord `shouldBe` Just (crMapKV (const testKey) (const testValue) testRecord)
