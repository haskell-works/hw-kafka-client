{-# LANGUAGE ScopedTypeVariables #-}
module ConsumerExample

where

import Control.Arrow  ((&&&))
import Data.Monoid    ((<>))
import Kafka.Consumer

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = brokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "consumer_example_group")
             <> noAutoCommit
             <> setCallback (rebalanceCallback printingRebalanceCallback)
             <> setCallback (offsetCommitCallback printingOffsetCallback)
             <> logLevel KafkaLogInfo

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics [TopicName "kafka-client-example-topic"]
           <> offsetReset Earliest

-- Running an example
runConsumerExample :: IO ()
runConsumerExample = do
    print $ cpLogLevel consumerProps
    res <- runConsumer consumerProps consumerSub processMessages
    print res

-------------------------------------------------------------------
processMessages :: KafkaConsumer -> IO (Either KafkaError ())
processMessages kafka = do
    mapM_ (\_ -> do
                   msg1 <- pollMessage kafka (Timeout 1000)
                   putStrLn $ "Message: " <> show msg1
                   err <- commitAllOffsets OffsetCommit kafka
                   putStrLn $ "Offsets: " <> maybe "Committed." show err
          ) [0 :: Integer .. 10]
    return $ Right ()

printingRebalanceCallback :: KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()
printingRebalanceCallback k e ps =
    case e of
        KafkaResponseError RdKafkaRespErrAssignPartitions -> do
            putStr "[Rebalance] Assign partitions: "
            mapM_ (print . (tpTopicName &&& tpPartition &&& tpOffset)) ps
            assign k ps >>= print
        KafkaResponseError RdKafkaRespErrRevokePartitions -> do
            putStr "[Rebalance] Revoke partitions: "
            mapM_ (print . (tpTopicName &&& tpPartition &&& tpOffset)) ps
            assign k [] >>= print
        x -> print "Rebalance: UNKNOWN (and unlikely!)" >> print x


printingOffsetCallback :: KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()
printingOffsetCallback _ e ps = do
    print ("Offsets callback:" ++ show e)
    mapM_ (print . (tpTopicName &&& tpPartition &&& tpOffset)) ps
