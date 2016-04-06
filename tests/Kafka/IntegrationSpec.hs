{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
( spec
) where

import           Control.Exception
import           Control.Monad.Loops
import           Data.Either
import           System.Environment

import           Kafka
import           Kafka.Consumer
import           Kafka.Producer

import           Test.Hspec

brokerAddress :: IO BrokersString
brokerAddress = BrokersString <$> getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> (return "localhost:9092")

testTopic :: IO TopicName
testTopic = TopicName <$> getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> (return "kafka-client_tests")

spec :: Spec
spec = describe "Kafka.IntegrationSpec" $ do
    it "sends messages to test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        res    <- runProducer broker emptyKafkaProps (sendMessages topic)
        res `shouldBe` [Nothing, Nothing]

    it "consumes messages from test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        res    <- runConsumer
                      (ConsumerGroupId "test_group_2")
                      broker
                      (KafkaProps [("enable.auto.commit", "false")])
                      (TopicProps [("auto.offset.reset", "earliest")])
                      [topic]
                      receiveMessages
        print $ show res
        length <$> res `shouldBe` Right 2

----------------------------------------------------------------------------------------------------------------

receiveMessages :: Kafka -> IO (Either a [ReceivedMessage])
receiveMessages kafka =
     (Right . rights) <$> www
     where
         www = whileJust maybeMsg return
         isOK msg = if msg /= Left (KafkaResponseError RdKafkaRespErrPartitionEof) then Just msg else Nothing
         maybeMsg = isOK <$> get
         get = do
             print "willPoll"
             x <- pollMessage kafka (Timeout 1000)
             print $ show x
             return x

testMessages :: [ProduceMessage]
testMessages =
    [ ProduceMessage "test from producer"
    , ProduceKeyedMessage "key" "test from producer (with key)"
    ]

sendMessages :: TopicName -> Kafka -> IO [Maybe KafkaError]
sendMessages (TopicName t) kafka = do
    print "send messages"
    topic <- newKafkaTopic kafka t emptyTopicProps
    mapM (produceMessage topic UnassignedPartition) testMessages
