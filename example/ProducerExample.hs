{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
where

import Control.Monad (forM_)
import Kafka
import Kafka.Producer

-- Global producer properties
producerProps :: ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = TopicName "kafka-client-example-topic"

-- Run an example
runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer producerProps sendMessages
    print $ show res

sendMessages :: KafkaProducer -> IO (Either KafkaError String)
sendMessages prod = do
  err1 <- produceMessage prod (ProducerRecord targetTopic UnassignedPartition "test from producer")
  forM_ err1 print

  err2 <- produceMessage prod (KeyedProducerRecord targetTopic "key" UnassignedPartition "test from producer (with key)")
  forM_ err2 print

  return $ Right "All done, Sir."
