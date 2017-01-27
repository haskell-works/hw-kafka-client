module ConsumerExample

where

import           Control.Arrow  ((&&&))
import           Data.Monoid ((<>))
import           Kafka
import           Kafka.Consumer

iterator :: [Integer]
iterator = [0 .. 20]

consumerProps :: ConsumerProperties
consumerProps = consumerBrokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "test_group")
             -- callbacks, unnecessary, demo only
             <> reballanceCallback (ReballanceCallback printingRebalanceCallback)
             <> offsetsCommitCallback (OffsetsCommitCallback printingOffsetCallback)

consumerSub :: Subscription
consumerSub = topics [TopicName "^hl-test*"]
           <> offsetReset Earliest
           <> noAutoCommit

runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer consumerProps
              consumerSub
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
