{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.TestEnv where

import Control.Exception
import Control.Monad      (void)
import Data.Monoid        ((<>))
import System.Environment
import System.IO.Unsafe

import Kafka.Consumer as C
import Kafka.Producer as P

import Test.Hspec

brokerAddress :: BrokerAddress
brokerAddress = unsafePerformIO $
  BrokerAddress <$> getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> (return "localhost:9092")
{-# NOINLINE brokerAddress #-}

testTopic :: TopicName
testTopic = unsafePerformIO $
  TopicName <$> getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> (return "kafka-client_tests")
{-# NOINLINE testTopic #-}

testGroupId :: ConsumerGroupId
testGroupId = ConsumerGroupId "it_spec_03"

consumerProps :: BrokerAddress -> ConsumerProperties
consumerProps broker = C.brokersList [broker]
                    <> groupId testGroupId
                    <> C.setCallback (logCallback (\l s1 s2 -> print $ show l <> ": " <> s1 <> ", " <> s2))
                    <> C.setCallback (errorCallback (\e r -> print $ show e <> ": " <> r))
                    <> noAutoCommit

consumerPropsNoStore :: BrokerAddress -> ConsumerProperties
consumerPropsNoStore broker = consumerProps broker <> noAutoOffsetStore

producerProps :: BrokerAddress -> ProducerProperties
producerProps broker = P.brokersList [broker]
                    <> P.setCallback (logCallback (\l s1 s2 -> print $ show l <> ": " <> s1 <> ", " <> s2))
                    <> P.setCallback (errorCallback (\e r -> print $ show e <> ": " <> r))

testSubscription :: TopicName -> Subscription
testSubscription t = topics [t]
              <> offsetReset Earliest

mkProducer :: IO KafkaProducer
mkProducer = do
    (Right p) <- newProducer (producerProps brokerAddress)
    return p

mkConsumerWith :: (BrokerAddress -> ConsumerProperties) -> IO KafkaConsumer
mkConsumerWith props = do
    (Right c) <- newConsumer (props brokerAddress) (testSubscription testTopic)
    return c

specWithConsumer :: String -> (BrokerAddress -> ConsumerProperties) -> SpecWith KafkaConsumer -> Spec
specWithConsumer s p f = beforeAll (mkConsumerWith p) $ afterAll (void . closeConsumer) $ describe s f

specWithProducer :: String -> SpecWith KafkaProducer -> Spec
specWithProducer s f = beforeAll mkProducer $ afterAll (void . closeProducer) $ describe s f
