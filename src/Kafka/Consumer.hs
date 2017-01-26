{-# LANGUAGE TupleSections #-}
module Kafka.Consumer
( module X
, runConsumerConf
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
, setDefaultTopicConf
, closeConsumer

-- ReExport Types
, TopicName (..)
, CIT.OffsetCommit (..)
, CIT.PartitionOffset (..)
, CIT.TopicPartition (..)
, CIT.ConsumerRecord (..)
, RDE.RdKafkaRespErrT (..)
)
where

import           Control.Exception
import qualified Data.ByteString as BS
import qualified Data.Map as M
import           Foreign                         hiding (void)
import           Kafka
import           Kafka.Consumer.ConsumerProperties
import           Kafka.Consumer.Convert
import           Kafka.Consumer.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared

import qualified Kafka.Consumer.Types   as CIT
import qualified Kafka.Internal.RdKafkaEnum      as RDE

import qualified Kafka.Types as X
import qualified Kafka.Consumer.ConsumerProperties as X


-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumerConf :: KafkaConf                            -- ^ Consumer config (see 'newConsumerConf')
                -> TopicConf                            -- ^ Topic config that is going to be used for every topic consumed by the consumer
                -> [BrokerAddress]                      -- ^ Comma separated list of brokers with ports (e.g. @localhost:9092@)
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
            return $ maybe (Right kafka) (Left . (, kafka)) sErr

        clConsumer (Left (_, kafka)) = maybeToLeft <$> closeConsumer kafka
        clConsumer (Right kafka) = maybeToLeft <$> closeConsumer kafka

        runHandler (Left (err, _)) = return $ Left err
        runHandler (Right kafka) = f kafka

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
runConsumer :: [BrokerAddress]
            -> ConsumerProperties
            -> TopicProps                           -- ^ Topic config that is going to be used for every topic consumed by the consumer             -> [TopicName]                          -- ^ List of topics to be consumed
            -> [TopicName]                          -- ^ List of topics to be consumed
            -> (Kafka -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
            -> IO (Either KafkaError a)
runConsumer bs cp tp ts f = do
    kc' <- newConsumerConf cp
    tc' <- newConsumerTopicConf tp
    runConsumerConf kc' tc' bs ts f

-- | Creates a new kafka configuration for a consumer with a specified 'ConsumerGroupId'.
newConsumerConf :: ConsumerProperties
                -> IO KafkaConf     -- ^ Kafka configuration which can be altered before it is used in 'newConsumer'
newConsumerConf (ConsumerProperties m) = kafkaConf (KafkaProps $ M.toList m)

newConsumerTopicConf :: TopicProps -> IO TopicConf
newConsumerTopicConf = topicConf

-- | Creates a new kafka consumer
newConsumer :: [BrokerAddress] -- ^ List of kafka brokers
            -> KafkaConf     -- ^ Kafka configuration for a consumer (see 'newConsumerConf')
            -> IO Kafka      -- ^ Kafka instance
newConsumer bs conf = do
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
        ps  <- fromNativeTopicPartitionList' pl
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
        ps  <- fromNativeTopicPartitionList' pl
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
subscribe :: Kafka -> [TopicName] -> IO (Maybe KafkaError)
subscribe (Kafka k _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl t (-1)) ts
    res <- KafkaResponseError <$> rdKafkaSubscribe k pl
    return $ kafkaErrorToMaybe res

-- | Polls the next message from a subscription
pollMessage :: Kafka
            -> Timeout -- ^ the timeout, in milliseconds (@10^3@ per second)
            -> IO (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))) -- ^ Left on error or timeout, right for success
pollMessage (Kafka k _) (Timeout ms) =
    rdKafkaConsumerPoll k (fromIntegral ms) >>= fromMessagePtr

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: Kafka                   -- ^ Kafka handle
                    -> OffsetCommit            -- ^ Offset commit mode, will block if `OffsetCommit`
                    -> ConsumerRecord k v
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
closeConsumer :: Kafka -> IO (Maybe KafkaError)
closeConsumer (Kafka k _) =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaConsumerClose k

setDefaultTopicConf :: KafkaConf -> TopicConf -> IO ()
setDefaultTopicConf (KafkaConf kc) (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

-----------------------------------------------------------------------------
commitOffsets :: Kafka -> OffsetCommit -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets (Kafka k _) o pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)
