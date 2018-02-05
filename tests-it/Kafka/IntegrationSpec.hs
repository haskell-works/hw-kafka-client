{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
where

import           Control.Monad       (forM_, forM)
import           Control.Monad.Loops
import qualified Data.ByteString     as BS
import           Data.Either
import           Data.Map

import Kafka.Consumer as C
import Kafka.Metadata as M
import Kafka.Producer as P

import Kafka.TestEnv
import Test.Hspec

{-# ANN module ("HLint: ignore Redundant do"  :: String) #-}

spec :: Spec
spec = do
    describe "Kafka.IntegrationSpec" $ do
        specWithProducer "Run producer" $ do
            it "sends messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer with per-message commit" $ do
            it "should receive 2 messages" $ \k -> do
                res <- receiveMessages k
                length <$> res `shouldBe` Right 2

                comRes <- forM res . mapM $ commitOffsetMessage OffsetCommit k
                comRes `shouldBe` Right [Nothing, Nothing]

        specWithProducer "Run producer again" $ do
            it "sends messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer after per-message commit" $ do
            it "should receive 2 messages again" $ \k -> do
                res <- receiveMessages k
                comRes <- commitAllOffsets OffsetCommit k

                length <$> res `shouldBe` Right 2
                comRes `shouldBe` Nothing

        specWithProducer "Run producer" $ do
            it "sends messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Run consumer" $ do
            it "should get committed" $ \k -> do
                res <- committed k (Timeout 10000) [(testTopic, PartitionId 0)]
                res `shouldSatisfy` isRight

            it "should get position" $ \k -> do
                res <- position k [(testTopic, PartitionId 0)]
                res `shouldSatisfy` isRight

            it "should receive messages" $ \k -> do
                res <- receiveMessages k
                length <$> res `shouldBe` Right 2

                let timestamps = crTimestamp <$> either (const []) id res
                forM_ timestamps $ \ts ->
                    ts `shouldNotBe` NoTimestamp

                comRes <- commitAllOffsets OffsetCommit k
                comRes `shouldBe` Nothing

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

            it "should seek and return no error" $ \k -> do
                res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) (PartitionOffset 1)]
                res `shouldBe` Nothing
                msg <- pollMessage k (Timeout 1000)
                crOffset <$> msg `shouldBe` Right (Offset 1)

            it "should seek to the beginning" $ \k -> do
                res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetBeginning]
                res `shouldBe` Nothing
                msg <- pollMessage k (Timeout 1000)
                crOffset <$> msg `shouldBe` Right (Offset 0)

            it "should seek to the end" $ \k -> do
                res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetEnd]
                res `shouldBe` Nothing
                msg <- pollMessage k (Timeout 1000)
                crOffset <$> msg `shouldSatisfy` (\x ->
                        x == Left (KafkaResponseError RdKafkaRespErrPartitionEof)
                    ||  x == Left (KafkaResponseError RdKafkaRespErrTimedOut))

            it "should respect out-of-bound offsets (invalid offset)" $ \k -> do
                res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetInvalid]
                res `shouldBe` Nothing
                msg <- pollMessage k (Timeout 1000)
                crOffset <$> msg `shouldBe` Right (Offset 0)

            it "should respect out-of-bound offsets (huge offset)" $ \k -> do
                res <- seek k (Timeout 1000) [TopicPartition testTopic (PartitionId 0) (PartitionOffset 123456)]
                res `shouldBe` Nothing
                msg <- pollMessage k (Timeout 1000)
                crOffset <$> msg `shouldBe` Right (Offset 0)


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
