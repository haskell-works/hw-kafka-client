{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
where

import Control.Monad (forM_)
import Data.Monoid
import Kafka.Producer
import Data.ByteString (ByteString)

-- Global producer properties
producerProps :: ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]
             <> producerLogLevel KafkaLogDebug

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = TopicName "kafka-client-example-topic"

mkMessage :: Maybe ByteString -> Maybe ByteString -> ProducerRecord
mkMessage k v = ProducerRecord
                  { prTopic = targetTopic
                  , prPartition = UnassignedPartition
                  , prKey = k
                  , prValue = v
                  }

-- Run an example
runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer producerProps sendMessages
    print res

sendMessages :: KafkaProducer -> IO (Either KafkaError ())
sendMessages prod = do
  err1 <- produceMessage prod (mkMessage Nothing (Just "test from producer") )
  forM_ err1 print

  err2 <- produceMessage prod (mkMessage (Just "key") (Just "test from producer (with key)"))
  forM_ err2 print

  errs <- produceMessageBatch prod
            [ mkMessage (Just "b-1") (Just "batch-1")
            , mkMessage (Just "b-2") (Just "batch-2")
            , mkMessage Nothing      (Just "batch-3")
            ]

  forM_ errs (print . snd)
  return $ Right ()
