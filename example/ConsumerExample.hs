module ConsumerExample

where

import           Control.Arrow  ((&&&))
import           Data.Monoid ((<>))
import           Kafka
import           Kafka.Consumer

iterator :: [Integer]
iterator = [0 .. 20]

consumerProps :: ConsumerProperties
consumerProps = groupId (ConsumerGroupId "test_group")
             <> offsetReset Earliest
             <> noAutoCommit

runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer [BrokerAddress "localhost:9092"] consumerProps
              emptyTopicProps
              [TopicName "^hl-test*"]
              processMessages
    print $ show res

consumerExample :: IO ()
consumerExample = do
    print "creating kafka conf"
    kafkaConf <- newConsumerConf consumerProps
    topicConf <- newConsumerTopicConf emptyTopicProps
    -- unnecessary, demo only
    setRebalanceCallback kafkaConf printingRebalanceCallback
    setOffsetCommitCallback kafkaConf printingOffsetCallback

    res <- runConsumerConf
               kafkaConf
               topicConf
               [BrokerAddress "localhost:9092"]
               [TopicName "kafka-client_tests"]
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

printingRebalanceCallback :: Kafka -> KafkaError -> [TopicPartition] -> IO ()
printingRebalanceCallback k e ps = do
    print $ show e
    mapM_ (print . show . (tpTopicName &&& tpPartition &&& tpOffset)) ps
    case e of
        KafkaResponseError RdKafkaRespErrAssignPartitions ->
            assign k ps >>= print . show
        KafkaResponseError RdKafkaRespErrRevokePartitions ->
            assign k [] >>= print . show
        x -> print "UNKNOWN (and unlikely!)" >> print (show x)


printingOffsetCallback :: Kafka -> KafkaError -> [TopicPartition] -> IO ()
printingOffsetCallback _ e ps = do
    print $ show e
    mapM_ (print . show . (tpTopicName &&& tpPartition &&& tpOffset)) ps
