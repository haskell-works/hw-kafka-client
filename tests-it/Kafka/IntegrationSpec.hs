{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
where

import           Control.Monad       (forM_)
import           Control.Monad.Loops
import qualified Data.ByteString     as BS
import           Data.Either
import           Data.Map

import Kafka.Consumer as C
import Kafka.Metadata as M
import Kafka.Producer as P

import Kafka.TestEnv
import Test.Hspec

spec :: Spec
spec = do
    describe "Kafka.IntegrationSpec" $ do
        specWithProducer "Run producer" $ do
            it "sends messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Run consumer" $ do
            it "should receive messages" $ \k -> do
                res <- receiveMessages k
                length <$> res `shouldBe` Right 2

                let timestamps = crTimestamp <$> either (const []) id res
                forM_ timestamps $ \ts ->
                    ts `shouldNotBe` NoTimestamp

            it "should get watermark offsets" $ \k -> do
                res <- sequence <$> watermarkOffsets k (Timeout 1000) testTopic
                res `shouldSatisfy` isRight
                length <$> res `shouldBe` (Right 1)

            it "should return subscription" $ \k -> do
                res <- subscription k
                res `shouldSatisfy` isRight
                length <$> res `shouldBe` Right 1

            it "should return assignment" $ \k -> do
                res <- assignment k
                res `shouldSatisfy` isRight
                res `shouldBe` Right (fromList [(testTopic, [PartitionId 0])])

            it "should return all topics metadata" $ \k -> do
                res <- allTopicsMetadata k (Timeout 1000)
                res `shouldSatisfy` isRight
                (length . kmBrokers) <$> res `shouldBe` Right 1
                (length . kmTopics) <$> res `shouldBe` Right 2

            it "should return topic metadata" $ \k -> do
                res <- topicMetadata k (Timeout 1000) testTopic
                res `shouldSatisfy` isRight
                (length . kmBrokers) <$> res `shouldBe` Right 1
                (length . kmTopics) <$> res `shouldBe` Right 1

            it "should describe all consumer groups" $ \k -> do
                res <- allConsumerGroupsInfo k (Timeout 1000)
                fmap giGroup <$> res `shouldBe` Right [testGroupId]

            it "should describe a given consumer group" $ \k -> do
                res <- consumerGroupInfo k (Timeout 1000) testGroupId
                fmap giGroup <$> res `shouldBe` Right [testGroupId]

            it "should describe non-existent consumer group" $ \k -> do
                res <- consumerGroupInfo k (Timeout 1000) (ConsumerGroupId "does-not-exist")
                res `shouldBe` Right []

            it "should read topic offsets for time" $ \k -> do
                res <- topicOffsetsForTime k (Timeout 1000) (Millis 1904057189508) testTopic
                res `shouldSatisfy` isRight
                fmap tpOffset <$> res `shouldBe` Right [PartitionOffsetEnd]

----------------------------------------------------------------------------------------------------------------

receiveMessages :: KafkaConsumer -> IO (Either KafkaError [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
receiveMessages kafka =
     (Right . rights) <$> www
     where
         www = whileJust maybeMsg return
         isOK msg = if msg /= Left (KafkaResponseError RdKafkaRespErrPartitionEof) then Just msg else Nothing
         maybeMsg = isOK <$> get
         get = pollMessage kafka (Timeout 1000)

testMessages :: TopicName -> [ProducerRecord]
testMessages t =
    [ ProducerRecord t UnassignedPartition Nothing (Just "test from producer")
    , ProducerRecord t UnassignedPartition (Just "key") (Just "test from producer (with key)")
    ]

sendMessages :: [ProducerRecord] -> KafkaProducer -> IO (Either KafkaError ())
sendMessages msgs prod =
  Right <$> forM_ msgs (produceMessage prod)
