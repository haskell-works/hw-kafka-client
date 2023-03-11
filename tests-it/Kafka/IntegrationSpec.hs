{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad           (forM, forM_, void)
import Control.Monad.Loops
import Data.Either
import Data.Map                (fromList)
import qualified Data.Set as Set
import Data.Monoid             ((<>))
import Kafka.Consumer
import Kafka.Metadata
import Kafka.Producer
import Kafka.TestEnv
import Test.Hspec

import qualified Data.ByteString as BS

{- HLINT ignore "Redundant do"  -}

spec :: Spec
spec = do
    describe "Per-message commit" $ do
        specWithProducer "Run producer" $ do
            it "1. sends 2 messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer with per-message commit" consumerProps $ do
            it "2. should receive 2 messages" $ \k -> do
                res <- receiveMessages k
                length <$> res `shouldBe` Right 2

                comRes <- forM res . mapM $ commitOffsetMessage OffsetCommit k
                comRes `shouldBe` Right [Nothing, Nothing]

        specWithProducer "Run producer again" $ do
            it "3. sends 2 messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer after per-message commit" consumerProps $ do
            it "4. should receive 2 messages again" $ \k -> do
                res <- receiveMessages k
                comRes <- commitAllOffsets OffsetCommit k

                length <$> res `shouldBe` Right 2
                comRes `shouldBe` Nothing

    describe "Store offsets" $ do
        specWithProducer "Run producer" $ do
            it "1. sends 2 messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer with no auto store" consumerPropsNoStore $ do
            it "2. should receive 2 messages without storing" $ \k -> do
                res <- receiveMessages k
                length <$> res `shouldBe` Right 2

                comRes <- commitAllOffsets OffsetCommit k
                comRes `shouldBe` Just (KafkaResponseError RdKafkaRespErrNoOffset)

        specWithProducer "Run producer again" $ do
            it "3. sends 2 messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer after commit without store" consumerPropsNoStore $ do
            it "4. should receive 4 messages and store them" $ \k -> do
                res <- receiveMessages k
                storeRes <- forM res . mapM $ storeOffsetMessage k
                comRes <- commitAllOffsets OffsetCommit k

                length <$> storeRes `shouldBe` Right 4
                length <$> res `shouldBe` Right 4
                comRes `shouldBe` Nothing

        specWithProducer "Run producer again" $ do
            it "5. sends 2 messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

        specWithConsumer "Consumer after commit with store" consumerPropsNoStore $ do
            it "6. should receive 2 messages" $ \k -> do
                res <- receiveMessages k
                storeRes <- forM res $ mapM (storeOffsetMessage k)
                comRes <- commitAllOffsets OffsetCommit k

                length <$> res `shouldBe` Right 2
                length <$> storeRes `shouldBe` Right 2
                comRes `shouldBe` Nothing

        specWithKafka "Part 3 - Consume after committing stored offsets" consumerPropsNoStore $ do
            it "5. sends 2 messages to test topic" $ \(_, prod) -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

            it "6. should receive 2 messages" $ \(k, _) -> do
                res <- receiveMessages k
                storeRes <- forM res $ mapM (storeOffsetMessage k)
                comRes <- commitAllOffsets OffsetCommit k

                length <$> res `shouldBe` Right 2
                length <$> storeRes `shouldBe` Right 2
                comRes `shouldBe` Nothing

    describe "Kafka.IntegrationSpec" $ do
        specWithProducer "Run producer" $ do
            it "sends messages to test topic" $ \prod -> do
                res    <- sendMessages (testMessages testTopic) prod
                res `shouldBe` Right ()

            it "sends messages with callback to test topic" $ \prod -> do
                var <- newEmptyMVar
                let
                  msg = ProducerRecord
                    { prTopic = "callback-topic"
                    , prPartition = UnassignedPartition
                    , prKey = Nothing
                    , prValue = Just "test from producer"
                    , prHeaders = mempty
                    }

                res <- produceMessage' prod msg (putMVar var)
                res `shouldBe` Right ()
                callbackRes <- flushProducer prod *> takeMVar var
                callbackRes `shouldSatisfy` \case
                  DeliverySuccess _ _ -> True
                  DeliveryFailure _ _ -> False
                  NoMessageError _    -> False

        specWithConsumer "Run consumer with async polling" (consumerProps <> groupId (makeGroupId "async")) runConsumerSpec
        specWithConsumer "Run consumer with sync polling" (consumerProps <> groupId (makeGroupId "sync") <> callbackPollMode CallbackPollModeSync) runConsumerSpec

    describe "Kafka.Consumer.BatchSpec" $ do
        specWithConsumer "Batch consumer" (consumerProps <> groupId "batch-consumer") $ do
            it "should consume first batch" $ \k -> do
                res <- pollMessageBatch k (Timeout 1000) (BatchSize 5)
                length res `shouldBe` 5
                forM_ res (`shouldSatisfy` isRight)

            it "should consume second batch with not enough messages" $ \k -> do
                res <- pollMessageBatch k (Timeout 1000) (BatchSize 50)
                let res' = Prelude.filter (/= Left (KafkaResponseError RdKafkaRespErrPartitionEof)) res
                length res' `shouldSatisfy` (< 50)
                forM_ res' (`shouldSatisfy` isRight)

            it "should consume empty batch when there are no messages" $ \k -> do
                res <- pollMessageBatch k (Timeout 1000) (BatchSize 50)
                length res `shouldBe` 0

    describe "Kafka.Headers.Spec" $ do
        let testHeaders = headersFromList [("a-header-name", "a-header-value"), ("b-header-name", "b-header-value")]

        specWithKafka "Headers consumer/producer" consumerProps $ do
              it "1. sends 2 messages to test topic enriched with headers" $ \(k, prod) -> do
                  void $ receiveMessages k

                  res  <- sendMessagesWithHeaders (testMessages testTopic) testHeaders prod
                  res `shouldBe` Right ()
              it "2. should receive 2 messages enriched with headers" $ \(k, _) -> do
                  res <- receiveMessages k
                  (length <$> res) `shouldBe` Right 2

                  forM_ res $ \rcs ->
                    forM_ rcs ((`shouldBe` Set.fromList (headersToList testHeaders)) . Set.fromList . headersToList . crHeaders)

----------------------------------------------------------------------------------------------------------------

data ReadState = Skip | Read

receiveMessages :: KafkaConsumer -> IO (Either KafkaError [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
receiveMessages kafka =
    Right . rights <$> allMessages
    where
        allMessages =
            unfoldrM (\s -> do
                msg <- pollMessage kafka (Timeout 1000)
                case (s, msg) of
                    (Skip, Left _)  -> pure $ Just (msg, Skip)
                    (_, Right msg') -> pure $ Just (Right msg', Read)
                    (Read, _)       -> pure Nothing

            ) Skip

testMessages :: TopicName -> [ProducerRecord]
testMessages t =
    [ ProducerRecord t UnassignedPartition Nothing (Just "test from producer") mempty
    , ProducerRecord t UnassignedPartition (Just "key") (Just "test from producer (with key)") mempty
    ]

sendMessages :: [ProducerRecord] -> KafkaProducer -> IO (Either KafkaError ())
sendMessages msgs prod =
  Right <$> (forM_ msgs (produceMessage prod) >> flushProducer prod)

sendMessagesWithHeaders :: [ProducerRecord] -> Headers -> KafkaProducer -> IO (Either KafkaError ())
sendMessagesWithHeaders msgs hdrs prod =
  Right <$> (forM_ msgs (\msg -> produceMessage prod (msg {prHeaders = hdrs})) >> flushProducer prod)

runConsumerSpec :: SpecWith KafkaConsumer
runConsumerSpec = do
  it "should receive messages" $ \k -> do
    res <- receiveMessages k
    let msgsLen = either (const 0) length res
    msgsLen `shouldSatisfy` (> 0)

    let timestamps = crTimestamp <$> either (const []) id res
    forM_ timestamps $ \ts ->
      ts `shouldNotBe` NoTimestamp

    comRes <- commitAllOffsets OffsetCommit k
    comRes `shouldBe` Nothing

  it "should get committed" $ \k -> do
    res <- committed k (Timeout 1000) [(testTopic, PartitionId 0)]
    res `shouldSatisfy` isRight

  it "should get position" $ \k -> do
    res <- position k [(testTopic, PartitionId 0)]
    res `shouldSatisfy` isRight

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
    let filterUserTopics m = m { kmTopics = filter (\t -> topicType (tmTopicName t) == User) (kmTopics m) }
    let res' = fmap filterUserTopics res
    length . kmBrokers <$> res' `shouldBe` Right 1

    let topicsLen = either (const 0) (length . kmTopics) res'
    let hasTopic = either (const False) (any (\t -> tmTopicName t == testTopic) . kmTopics) res'

    topicsLen `shouldSatisfy` (>0)
    hasTopic `shouldBe` True

  it "should return topic metadata" $ \k -> do
    res <- topicMetadata k (Timeout 2000) testTopic
    res `shouldSatisfy` isRight
    length . kmBrokers <$> res `shouldBe` Right 1
    length . kmTopics <$> res `shouldBe` Right 1

  it "should describe all consumer groups" $ \k -> do
    res <- allConsumerGroupsInfo k (Timeout 2000)
    res `shouldSatisfy` isRight
    let groups = either (const []) (fmap giGroup) res
    let prefixedGroups = filter isTestGroupId groups
    let resLen = length prefixedGroups
    resLen `shouldSatisfy` (>0)
    -- fmap giGroup <$> res `shouldBe` Right [testGroupId]

  it "should describe a given consumer group" $ \k -> do
    res <- consumerGroupInfo k (Timeout 2000) testGroupId
    fmap giGroup <$> res `shouldBe` Right [testGroupId]

  it "should describe non-existent consumer group" $ \k -> do
    res <- consumerGroupInfo k (Timeout 2000) "does-not-exist"
    res `shouldBe` Right []

  it "should read topic offsets for time" $ \k -> do
    res <- topicOffsetsForTime k (Timeout 2000) (Millis 1904057189508) testTopic
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
    res <- seek k (Timeout 2000) [TopicPartition testTopic (PartitionId 0) PartitionOffsetInvalid]
    res `shouldBe` Nothing
    msg <- pollMessage k (Timeout 1000)
    crOffset <$> msg `shouldBe` Right (Offset 0)

  it "should respect out-of-bound offsets (huge offset)" $ \k -> do
    res <- seek k (Timeout 3000) [TopicPartition testTopic (PartitionId 0) (PartitionOffset 123456)]
    res `shouldBe` Nothing
    msg <- pollMessage k (Timeout 2000)
    crOffset <$> msg `shouldBe` Right (Offset 0)
