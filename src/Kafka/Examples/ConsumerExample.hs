module Kafka.Examples.ConsumerExample

where

import           Control.Arrow  ((&&&))
import           Kafka
import           Kafka.Consumer

iterator :: [Integer]
iterator = [0 .. 20]

runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer
              (ConsumerGroupId "test_group")
              []
              (BrokersString "localhost:9092")
              [TopicName "^hl-test*"]
              processMessages
    print $ show res

consumerExample :: IO ()
consumerExample = do
    print "creating kafka conf"
    conf <- newKafkaConsumerConf (ConsumerGroupId "test_group") []

    -- unnecessary, demo only
    setRebalanceCallback conf printingRebalanceCallback
    setOffsetCommitCallback conf printingOffsetCallback

    res <- runConsumerConf
               conf
               (BrokersString "localhost:9092")
               [TopicName "^hl-test*"]
               processMessages

    print $ show res

-------------------------------------------------------------------
processMessages :: Kafka -> IO (Either KafkaError ())
processMessages kafka = do
    mapM_ (\_ -> do
                   msg1 <- pollMessage kafka (Timeout 1000)
                   print $ show msg1
                   err <- commitAllOffsets kafka OffsetCommit
                   print $ show err) iterator
    return $ Right ()

printingRebalanceCallback :: Kafka -> KafkaError -> [KafkaTopicPartition] -> IO ()
printingRebalanceCallback k e ps = do
    print $ show e
    mapM_ (print . show . (ktpTopicName &&& ktpPartition &&& ktpOffset)) ps
    case e of
        KafkaResponseError RdKafkaRespErrAssignPartitions ->
            assign k ps >>= print . show
        KafkaResponseError RdKafkaRespErrRevokePartitions ->
            assign k [] >>= print . show
        x -> print "UNKNOWN (and unlikely!)" >> print (show x)


printingOffsetCallback :: Kafka -> KafkaError -> [KafkaTopicPartition] -> IO ()
printingOffsetCallback _ e ps = do
    print $ show e
    mapM_ (print . show . (ktpTopicName &&& ktpPartition &&& ktpOffset)) ps
