{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
where

import           Kafka
import           Kafka.Producer


producerProps ::ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]

runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer producerProps sendMessages
    print $ show res

sendMessages :: Kafka -> IO String
sendMessages kafka = do
    topic <- newKafkaTopic kafka "hl-test" emptyTopicProps
    err1 <- produceMessage topic (ProduceMessage UnassignedPartition "test from producer")
    print $ show err1

    err2 <- produceMessage topic (ProduceKeyedMessage "key" UnassignedPartition "test from producer (with key)")
    print $ show err2

    return "All done, Sir."
