module Kafka.Consumer
( runConsumerConf
, runConsumer
, newConsumerConf
, newConsumerTopicConf
, newConsumer
, setRebalanceCallback
, assign
, subscribe
, pollMessage
, commitOffsetMessage
, commitAllOffsets
, setOffsetCommitCallback
, closeConsumer

-- ReExport Types
, CIT.ConsumerGroupId (..)
, CIT.TopicName (..)
, CIT.OffsetCommit (..)
, CIT.PartitionOffset (..)
, CIT.TopicPartition (..)
, CIT.ReceivedMessage (..)
, RDE.RdKafkaRespErrT (..)
)
where

import           Control.Exception
import           Foreign                         hiding (void)
import           Kafka
import           Kafka.Consumer.Internal.Convert
import           Kafka.Consumer.Internal.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared

import qualified Kafka.Consumer.Internal.Types   as CIT
import qualified Kafka.Internal.RdKafkaEnum      as RDE

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumerConf :: KafkaConf                            -- ^ Consumer config (see 'newConsumerConf')
                -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
                -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
                -> [TopicName]                          -- ^ List of topics to be consumed
                -> (Kafka -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
                -> IO (Either KafkaError a)
runConsumerConf kc tc bs ts f =
    bracket mkConsumer clConsumer runHandler
    where
        mkConsumer = do
            setDefaultTopicConf kc tc
            kafka <- newConsumer bs kc
            sErr  <- subscribe kafka ts
            return $ if hasError sErr
                         then Left (sErr, kafka)
                         else Right kafka

        clConsumer (Left (_, kafka)) = kafkaErrorToEither <$> closeConsumer kafka
        clConsumer (Right kafka) = kafkaErrorToEither <$> closeConsumer kafka

        runHandler (Left (err, _)) = return $ Left err
        runHandler (Right kafka) = f kafka

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumer :: ConsumerGroupId                       -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
             -> BrokersString                        -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
             -> KafkaProps                           -- ^ Extra kafka consumer parameters (see kafka documentation)
             -> TopicProps                           -- ^ Topic config that is going to be used for every topic consumed by the consumer             -> [TopicName]                          -- ^ List of topics to be consumed
             -> [TopicName]                          -- ^ List of topics to be consumed
             -> (Kafka -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
             -> IO (Either KafkaError a)
runConsumer g bs kc tc ts f = do
    kc' <- newConsumerConf g kc
    tc' <- newConsumerTopicConf tc
    runConsumerConf kc' tc' bs ts f

-- | Creates a new kafka configuration for a consumer with a specified 'ConsumerGroupId'.
newConsumerConf :: ConsumerGroupId  -- ^ Consumer group id (a @group.id@ property of a kafka consumer)
                     -> KafkaProps       -- ^ Extra kafka consumer parameters (see kafka documentation)
                     -> IO KafkaConf     -- ^ Kafka configuration which can be altered before it is used in 'newConsumer'
newConsumerConf (ConsumerGroupId gid) conf = do
    kc <- kafkaConf conf
    setKafkaConfValue kc "group.id" gid
    return kc

newConsumerTopicConf :: TopicProps -> IO TopicConf
newConsumerTopicConf = topicConf

-- | Creates a new kafka consumer
newConsumer :: BrokersString -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
                 -> KafkaConf     -- ^ Kafka configuration for a consumer (see 'newConsumerConf')
                 -> IO Kafka      -- ^ Kafka instance
newConsumer (BrokersString bs) conf = do
    kafka <- newKafkaPtr RdKafkaConsumer conf
    addBrokers kafka bs
    return kafka

-- | Sets a callback that is called when rebalance is needed.
--
-- Callback implementations suppose to watch for 'KafkaResponseError' 'RdKafkaRespErrAssignPartitions' and
-- for 'KafkaResponseError' 'RdKafkaRespErrRevokePartitions'. Other error codes are not expected and would indicate
-- something really bad happening in a system, or bugs in @librdkafka@ itself.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
--     * When 'RdKafkaRespErrAssignPartitions' happens 'assign' should be called with all the partitions it was called with.
--       It is OK to alter partitions offsets before calling 'assign'.
--
--     * When 'RdKafkaRespErrRevokePartitions' happens 'assign' should be called with an empty list of partitions.
setRebalanceCallback :: KafkaConf
                     -> (Kafka -> KafkaError -> [TopicPartition] -> IO ())
                     -> IO ()
setRebalanceCallback (KafkaConf conf) callback = rdKafkaConfSetRebalanceCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        pl' <- peek pl
        ps  <- fromNativeTopicPartitionList pl'
        callback (Kafka rk' (KafkaConf conf)) (KafkaResponseError err) ps

-- | Sets a callback that is called when rebalance is needed.
--
-- The results of automatic or manual offset commits will be scheduled
-- for this callback and is served by `pollMessage`.
--
-- A callback is expected to call 'assign' according to the error code it receives.
--
-- If no partitions had valid offsets to commit this callback will be called
-- with `KafkaError` == `KafkaResponseError` `RdKafkaRespErrNoOffset` which is not to be considered
-- an error.
setOffsetCommitCallback :: KafkaConf
                        -> (Kafka -> KafkaError -> [TopicPartition] -> IO ())
                        -> IO ()
setOffsetCommitCallback (KafkaConf conf) callback = rdKafkaConfSetOffsetCommitCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        pl' <- peek pl
        ps  <- fromNativeTopicPartitionList pl'
        callback (Kafka rk' (KafkaConf conf)) (KafkaResponseError err) ps

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
-- See 'setRebalanceCallback' for more details.
assign :: Kafka -> [TopicPartition] -> IO KafkaError
assign (Kafka k _) ps =
    let pl = if null ps
                then newForeignPtr_ nullPtr
                else toNativeTopicPartitionList ps
    in  KafkaResponseError <$> (pl >>= rdKafkaAssign k)

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: Kafka -> [TopicName] -> IO KafkaError
subscribe (Kafka k _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl t (-1)) ts
    KafkaResponseError <$> rdKafkaSubscribe k pl

-- | Polls the next message from a subscription
pollMessage :: Kafka
            -> Timeout -- ^ the timeout, in milliseconds (@10^3@ per second)
            -> IO (Either KafkaError ReceivedMessage) -- ^ Left on error or timeout, right for success
pollMessage (Kafka k _) (Timeout ms) =
    pollRdKafkaConsumer k (fromIntegral ms) >>= fromMessagePtr

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: Kafka                   -- ^ Kafka handle
                    -> OffsetCommit            -- ^ Offset commit mode, will block if `OffsetCommit`
                    -> ReceivedMessage
                    -> IO (Maybe KafkaError)
commitOffsetMessage k o m =
    toNativeTopicPartitionList [topicPartitionFromMessage m] >>= commitOffsets k o

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets :: Kafka                      -- ^ Kafka handle
                 -> OffsetCommit               -- ^ Offset commit mode, will block if `OffsetCommit`
                 -> IO (Maybe KafkaError)
commitAllOffsets k o =
    newForeignPtr_ nullPtr >>= commitOffsets k o

-- | Closes the consumer and destroys it.
closeConsumer :: Kafka -> IO KafkaError
closeConsumer (Kafka k _) =
    KafkaResponseError <$> rdKafkaConsumerClose k

-----------------------------------------------------------------------------
setDefaultTopicConf :: KafkaConf -> TopicConf -> IO ()
setDefaultTopicConf (KafkaConf kc) (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: Kafka -> OffsetCommit -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets (Kafka k _) o pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)
