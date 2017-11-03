module Kafka.Consumer.Callbacks
( rebalanceCallback
, offsetCommitCallback
, module X
)
where

import Foreign
import Kafka.Callbacks        as X
import Kafka.Consumer.Convert
import Kafka.Consumer.Types
import Kafka.Internal.RdKafka
import Kafka.Types

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
rebalanceCallback :: (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()) -> KafkaConf -> IO ()
rebalanceCallback callback (KafkaConf conf ct) = rdKafkaConfSetRebalanceCb conf realCb
  where
    realCb k err pl = do
      k' <- newForeignPtr_ k
      pls <- fromNativeTopicPartitionList' pl
      callback (KafkaConsumer (Kafka k') (KafkaConf conf ct)) (KafkaResponseError err) pls

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
offsetCommitCallback :: (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()) -> KafkaConf -> IO ()
offsetCommitCallback callback (KafkaConf conf ct) = rdKafkaConfSetOffsetCommitCb conf realCb
  where
    realCb k err pl = do
      k' <- newForeignPtr_ k
      pls <- fromNativeTopicPartitionList' pl
      callback (KafkaConsumer (Kafka k') (KafkaConf conf ct)) (KafkaResponseError err) pls

