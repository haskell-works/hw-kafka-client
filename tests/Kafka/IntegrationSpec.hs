{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
( spec
) where

import           Control.Exception
import           Control.Monad.Loops
import           Data.Monoid ((<>))
import           Data.Either
import           System.Environment
import qualified Data.ByteString as BS

import           Kafka
import           Kafka.Consumer
import           Kafka.Producer

import           Test.Hspec

brokerAddress :: IO BrokerAddress
brokerAddress = BrokerAddress <$> getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> (return "localhost:9092")

testTopic :: IO TopicName
testTopic = TopicName <$> getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> (return "kafka-client_tests")

consumerProps :: BrokerAddress -> ConsumerProperties
consumerProps broker = consumerBrokersList [broker]
                    <> groupId (ConsumerGroupId "it_spec_01")
                    <> noAutoCommit

producerProps :: BrokerAddress -> ProducerProperties
producerProps broker = producerBrokersList [broker]

subscription :: TopicName -> Subscription
subscription t = topics [t]
              <> offsetReset Earliest

spec :: Spec
spec = describe "Kafka.IntegrationSpec" $ do
    it "sends messages to test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        res    <- runProducer (producerProps broker) (sendMessages topic)
        res `shouldBe` [Nothing, Nothing]

    it "consumes messages from test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        res    <- runConsumer
                      (consumerProps broker)
                      (subscription topic)
                      receiveMessages
        print $ show res
        length <$> res `shouldBe` Right 2

----------------------------------------------------------------------------------------------------------------

receiveMessages :: Kafka -> IO (Either a [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
receiveMessages kafka =
     (Right . rights) <$> www
     where
         www = whileJust maybeMsg return
         isOK msg = if msg /= Left (KafkaResponseError RdKafkaRespErrPartitionEof) then Just msg else Nothing
         maybeMsg = isOK <$> get
         get = do
             x <- pollMessage kafka (Timeout 1000)
             print $ show x
             return x

testMessages :: [ProduceMessage]
testMessages =
    [ ProduceMessage UnassignedPartition "test from producer"
    , ProduceKeyedMessage "key" UnassignedPartition "test from producer (with key)"
    ]

sendMessages :: TopicName -> Kafka -> IO [Maybe KafkaError]
sendMessages (TopicName t) kafka = do
    topic <- newKafkaTopic kafka t emptyTopicProps
    mapM (produceMessage topic) testMessages
