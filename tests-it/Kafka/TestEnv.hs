{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kafka.TestEnv where

import           Control.Exception
import           Control.Monad      (void)
import           Data.Monoid        ((<>))
import qualified Data.Text          as Text
import           System.Environment
import           System.IO.Unsafe

import qualified System.Random as Rnd

import Control.Concurrent
import Kafka.Consumer     as C
import Kafka.Producer     as P

import Test.Hspec

testPrefix :: String
testPrefix = unsafePerformIO $ take 10 . Rnd.randomRs ('a','z') <$> Rnd.newStdGen
{-# NOINLINE testPrefix #-}

brokerAddress :: BrokerAddress
brokerAddress = unsafePerformIO $
  (BrokerAddress . Text.pack) <$> getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> return "localhost:9092"
{-# NOINLINE brokerAddress #-}

testTopic :: TopicName
testTopic = unsafePerformIO $
  (TopicName . Text.pack) <$> getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> return $ testPrefix <> "-topic"
{-# NOINLINE testTopic #-}

testGroupId :: ConsumerGroupId
testGroupId = ConsumerGroupId (Text.pack testPrefix)

makeGroupId :: String -> ConsumerGroupId
makeGroupId suffix =
  ConsumerGroupId . Text.pack $ testPrefix <> "-" <> suffix

isTestGroupId :: ConsumerGroupId -> Bool
isTestGroupId (ConsumerGroupId group) = Text.pack testPrefix `Text.isPrefixOf` group

consumerProps :: ConsumerProperties
consumerProps =  C.brokersList [brokerAddress]
              <> groupId testGroupId
              <> C.setCallback (logCallback (\l s1 s2 -> print $ "[Consumer] " <> show l <> ": " <> s1 <> ", " <> s2))
              <> C.setCallback (errorCallback (\e r -> print $ "[Consumer] " <> show e <> ": " <> r))
              <> noAutoCommit

consumerPropsNoStore :: ConsumerProperties
consumerPropsNoStore = consumerProps <> noAutoOffsetStore

producerProps :: ProducerProperties
producerProps =  P.brokersList [brokerAddress]
              <> P.setCallback (logCallback (\l s1 s2 -> print $ "[Producer] " <> show l <> ": " <> s1 <> ", " <> s2))
              <> P.setCallback (errorCallback (\e r -> print $ "[Producer] " <> show e <> ": " <> r))

testSubscription :: TopicName -> Subscription
testSubscription t = topics [t]
              <> offsetReset Earliest

mkProducer :: IO KafkaProducer
mkProducer = newProducer producerProps >>= \(Right p) -> pure p

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
specWithConsumer s p f =
  beforeAll (mkConsumerWith p)
  $ afterAll (void . closeConsumer)
  $ describe s f

specWithProducer :: String -> SpecWith KafkaProducer -> Spec
specWithProducer s f = beforeAll mkProducer $ afterAll (void . closeProducer) $ describe s f

specWithKafka :: String -> ConsumerProperties -> SpecWith (KafkaConsumer, KafkaProducer) -> Spec
specWithKafka s p f =
  beforeAll ((,) <$> mkConsumerWith p <*> mkProducer)
    $ afterAll (\(consumer, producer) -> void $ closeProducer producer >> closeConsumer consumer)
    $ describe s f
