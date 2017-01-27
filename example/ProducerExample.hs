{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
where

import           Kafka
import           Kafka.Producer


producerProps ::ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]

targetTopic :: TopicName
targetTopic = TopicName "producer-example-topic"

runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer producerProps sendMessages
    print $ show res

sendMessages :: KafkaProducer -> IO (Either KafkaError String)
sendMessages prod = do
  err1 <- produceMessage prod (ProducerRecord targetTopic UnassignedPartition "test from producer")
  print $ show err1

  err2 <- produceMessage prod (KeyedProducerRecord targetTopic "key" UnassignedPartition "test from producer (with key)")
  print $ show err2

  return $ Right "All done, Sir."
