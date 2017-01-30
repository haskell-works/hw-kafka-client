{-# LANGUAGE ScopedTypeVariables #-}
module ConsumerExample

where

import Control.Arrow  ((&&&))
import Data.Monoid ((<>))
import Kafka
import Kafka.Consumer

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = consumerBrokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "consumer_example_group")
             <> noAutoCommit
             <> reballanceCallback (ReballanceCallback printingRebalanceCallback)
             <> offsetsCommitCallback (OffsetsCommitCallback printingOffsetCallback)

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
                   err <- commitAllOffsets OffsetCommit kafka
                   print $ "Offsets: " <> maybe "Committed." show err
          ) [0 :: Integer .. 20]
    return $ Right ()

printingRebalanceCallback :: KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()
printingRebalanceCallback k e ps = do
    putStrLn "Rebalance callback!"
    print ("Rebalance Error: " ++ show e)
    mapM_ (print . show . (tpTopicName &&& tpPartition &&& tpOffset)) ps
    case e of
        KafkaResponseError RdKafkaRespErrAssignPartitions ->
            assign k ps >>= print . show
        KafkaResponseError RdKafkaRespErrRevokePartitions ->
            assign k [] >>= print . show
        x -> print "UNKNOWN (and unlikely!)" >> print (show x)


printingOffsetCallback :: KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()
printingOffsetCallback _ e ps = do
    putStrLn "Offsets callback!"
    print ("Offsets Error:" ++ show e)
    mapM_ (print . show . (tpTopicName &&& tpPartition &&& tpOffset)) ps
