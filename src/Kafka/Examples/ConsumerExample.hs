module Kafka.Examples.ConsumerExample

where

import           Control.Arrow              ((&&&))
import           Kafka
import           Kafka.Consumer
import           Kafka.Internal.RdKafkaEnum

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
                   msg1 <- pollMessage kafka 1000
                   print $ show msg1) iterator
    return $ Right ()

printingRebalanceCallback :: Kafka -> KafkaError -> [KafkaTopicPartition] -> IO ()
printingRebalanceCallback k e ps = do
    print $ show e
    print "partitions: "
    mapM_ (print . show . (ktpTopicName &&& ktpPartition &&& ktpOffset)) ps
    case e of
        KafkaResponseError RdKafkaRespErrAssignPartitions -> do
            err <- assign k ps
            print $ "Assign result: " ++ show err
        KafkaResponseError RdKafkaRespErrRevokePartitions -> do
            err <- assign k []
            print $ "Revoke result: " ++ show err
        x ->
            print "UNKNOWN (and unlikely!)" >> print (show x)


