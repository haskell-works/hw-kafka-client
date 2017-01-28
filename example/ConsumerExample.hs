{-# LANGUAGE ScopedTypeVariables #-}
module ConsumerExample

where

import Data.Monoid ((<>))
import Kafka
import Kafka.Consumer

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = consumerBrokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "consumer_example_group")
             <> noAutoCommit

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics [TopicName "kafka-client-example-topic"]
           <> offsetReset Earliest

runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer consumerProps consumerSub processMessages
    print $ show res

-------------------------------------------------------------------
processMessages :: KafkaConsumer -> IO (Either KafkaError ())
processMessages kafka = do
    mapM_ (\_ -> do
                   msg1 <- pollMessage kafka (Timeout 1000)
                   print $ "Message: " <> show msg1
                   err <- commitAllOffsets kafka OffsetCommit
                   print $ "Offsets: " <> maybe "Committed." show err
          ) [0 :: Integer .. 20]
    return $ Right ()
