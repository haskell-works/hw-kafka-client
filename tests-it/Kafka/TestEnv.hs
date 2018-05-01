{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.TestEnv where

import Control.Exception
import Control.Monad      (void)
import Data.Monoid        ((<>))
import System.Environment
import System.IO.Unsafe

import Control.Concurrent
import Kafka.Consumer     as C
import Kafka.Producer     as P

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

consumerProps :: ConsumerProperties
consumerProps =  C.brokersList [brokerAddress]
              <> groupId testGroupId
              <> C.setCallback (logCallback (\l s1 s2 -> print $ show l <> ": " <> s1 <> ", " <> s2))
              <> C.setCallback (errorCallback (\e r -> print $ show e <> ": " <> r))
              <> noAutoCommit

consumerPropsNoStore :: ConsumerProperties
consumerPropsNoStore = consumerProps <> noAutoOffsetStore

producerProps :: ProducerProperties
producerProps =  P.brokersList [brokerAddress]
              <> P.setCallback (logCallback (\l s1 s2 -> print $ show l <> ": " <> s1 <> ", " <> s2))
              <> P.setCallback (errorCallback (\e r -> print $ show e <> ": " <> r))

testSubscription :: TopicName -> Subscription
testSubscription t = topics [t]
              <> offsetReset Earliest

mkProducer :: IO KafkaProducer
mkProducer = do
    (Right p) <- newProducer producerProps
    return p

mkConsumerWith :: ConsumerProperties -> IO KafkaConsumer
mkConsumerWith props = do
  waitVar <- newEmptyMVar
  let props' = props <> C.setCallback (rebalanceCallback (\_ -> rebCallback waitVar))
  (Right c) <- newConsumer props' (testSubscription testTopic)
  _ <- readMVar waitVar
  return c
  where
    rebCallback var evt = case evt of
      (RebalanceAssign _) -> putMVar var True
      _                   -> pure ()


specWithConsumer :: String -> ConsumerProperties -> SpecWith KafkaConsumer -> Spec
specWithConsumer s p f = beforeAll (mkConsumerWith p) $ afterAll (void . closeConsumer) $ describe s f

specWithProducer :: String -> SpecWith KafkaProducer -> Spec
specWithProducer s f = beforeAll mkProducer $ afterAll (void . closeProducer) $ describe s f

specWithKafka :: String -> ConsumerProperties -> SpecWith (KafkaConsumer, KafkaProducer) -> Spec
specWithKafka s p f =
  beforeAll ((,) <$> mkConsumerWith p <*> mkProducer)
    $ afterAll (\(consumer, producer) -> void $ closeProducer producer >> closeConsumer consumer)
    $ describe s f
