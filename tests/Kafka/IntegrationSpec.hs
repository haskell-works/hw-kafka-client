{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.IntegrationSpec
( spec
) where

import           Control.Exception
import           Control.Monad       (forM_)
import           Control.Monad.Loops
import qualified Data.ByteString     as BS
import           Data.Either
import           Data.Monoid         ((<>))
import           System.Environment

import Kafka
import Kafka.Consumer.Metadata

import Test.Hspec

brokerAddress :: IO BrokerAddress
brokerAddress = BrokerAddress <$> getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> (return "localhost:9092")

testTopic :: IO TopicName
testTopic = TopicName <$> getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> (return "kafka-client_tests")

consumerProps :: BrokerAddress -> ConsumerProperties
consumerProps broker = consumerBrokersList [broker]
                    <> groupId (ConsumerGroupId "it_spec_02")
                    <> noAutoCommit

producerProps :: BrokerAddress -> ProducerProperties
producerProps broker = producerBrokersList [broker]

testSubscription :: TopicName -> Subscription
testSubscription t = topics [t]
              <> offsetReset Earliest

spec :: Spec
spec = describe "Kafka.IntegrationSpec" $ do
    it "sends messages to test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        let msgs = testMessages topic
        res    <- runProducer (producerProps broker) (sendMessages msgs)
        res `shouldBe` Right ()

    it "consumes messages from test topic" $ do
        broker <- brokerAddress
        topic  <- testTopic
        res    <- runConsumer
                      (consumerProps broker)
                      (testSubscription topic)
                      (\k -> do
                        msgs <- receiveMessages k

                        {- Somehow this fails with "Assertion failed: (r == 0), function rwlock_wrlock, file tinycthread.c, line 1011." -}
                        wOffsets <- watermarkOffsets k topic
                        print wOffsets
                        sub  <- subscription k
                        sub `shouldSatisfy` isRight
                        length <$> sub `shouldBe` Right 1

                        {-  Somehow this fails with "Assertion failed: (r == 0), function rwlock_wrlock, file tinycthread.c, line 1011." -}
                        asgm <- assignment k
                        asgm `shouldSatisfy` isRight
                        length <$> asgm `shouldBe` Right 1

                        {-  Somehow this fails with "Assertion failed: (r == 0), function rwlock_wrlock, file tinycthread.c, line 1011." -}
                        allMeta <- allTopicsMetadata k
                        allMeta `shouldSatisfy` isRight
                        (length . kmBrokers) <$> allMeta `shouldBe` Right 1
                        (length . kmTopics) <$> allMeta `shouldBe` Right 2

                        {- This is fine, it works and doesn't fail -}
                        tMeta <- topicMetadata k (TopicName "oho")
                        tMeta `shouldSatisfy` isRight
                        (length . kmBrokers) <$> tMeta `shouldBe` Right 1
                        (length . kmTopics) <$> tMeta `shouldBe` Right 1

                        return msgs
                      )

        length <$> res `shouldBe` Right 2

        let timestamps = crTimestamp <$> either (const []) id res
        forM_ timestamps $ \ts ->
            ts `shouldNotBe` NoTimestamp

    it "Integration spec is finished" $ True `shouldBe` True

----------------------------------------------------------------------------------------------------------------

receiveMessages :: KafkaConsumer -> IO (Either a [ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)])
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

testMessages :: TopicName -> [ProducerRecord]
testMessages t =
    [ ProducerRecord t UnassignedPartition Nothing (Just "test from producer")
    , ProducerRecord t UnassignedPartition (Just "key") (Just "test from producer (with key)")
    ]

sendMessages :: [ProducerRecord] -> KafkaProducer -> IO (Either KafkaError ())
sendMessages msgs prod =
  Right <$> forM_ msgs (produceMessage prod)
