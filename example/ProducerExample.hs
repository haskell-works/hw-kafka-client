{-# LANGUAGE OverloadedStrings #-}

module ProducerExample
where

import Control.Monad         (forM_)
import Data.ByteString       (ByteString)
import Data.ByteString.Char8 (pack)
import Data.Monoid
import Kafka.Producer

-- Global producer properties
producerProps :: ProducerProperties
producerProps = brokersList [BrokerAddress "localhost:9092"]
             <> sendTimeout (Timeout 10000)
             <> setCallback (deliveryErrorsCallback print)
             <> logLevel KafkaLogDebug

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
  putStrLn "Producer is ready, send your messages!"
  msg1 <- getLine

  err1 <- produceMessage prod (mkMessage Nothing (Just $ pack msg1))
  forM_ err1 print

  putStrLn "One more time!"
  msg2 <- getLine

  err2 <- produceMessage prod (mkMessage (Just "key") (Just $ pack msg2))
  forM_ err2 print

  putStrLn "And the last one..."
  msg3 <- getLine
  err3 <- produceMessage prod (mkMessage (Just "key3") (Just $ pack msg3))

  -- errs <- produceMessageBatch prod
  --           [ mkMessage (Just "b-1") (Just "batch-1")
  --           , mkMessage (Just "b-2") (Just "batch-2")
  --           , mkMessage Nothing      (Just "batch-3")
  --           ]

  -- forM_ errs (print . snd)

  putStrLn "Thank you."
  return $ Right ()
