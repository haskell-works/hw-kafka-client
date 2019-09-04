{-# LANGUAGE BangPatterns #-}
module Kafka.Consumer.Callbacks
( rebalanceCallback
, offsetCommitCallback
, module X
)
where

import           Control.Arrow          ((&&&))
import           Control.Concurrent     (threadDelay)
import           Control.Monad          (forM_, void)
import           Data.Monoid            ((<>))
import qualified Data.Text              as Text
import           Foreign.ForeignPtr     (newForeignPtr_)
import           Foreign.Ptr            (nullPtr)
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Convert (fromNativeTopicPartitionList', fromNativeTopicPartitionList'', toNativeTopicPartitionList)
import           Kafka.Consumer.Types   (KafkaConsumer(..), TopicPartition(..), RebalanceEvent(..))
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup   (Kafka(..), KafkaConf(..), HasKafka(..), HasKafkaConf(..), getRdMsgQueue)
import           Kafka.Internal.Shared  (kafkaErrorToMaybe)
import           Kafka.Types            (KafkaError(..), PartitionId(..), TopicName(..))

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
-- rebalanceCallback :: (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()) -> KafkaConf -> IO ()
rebalanceCallback :: (KafkaConsumer -> RebalanceEvent -> IO ()) -> KafkaConf -> IO ()
rebalanceCallback callback kc@(KafkaConf conf _ _) = rdKafkaConfSetRebalanceCb conf realCb
  where
    realCb k err pl = do
      k' <- newForeignPtr_ k
      pls <- newForeignPtr_ pl
      setRebalanceCallback callback (KafkaConsumer (Kafka k') kc) (KafkaResponseError err) pls

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
offsetCommitCallback callback kc@(KafkaConf conf _ _) = rdKafkaConfSetOffsetCommitCb conf realCb
  where
    realCb k err pl = do
      k' <- newForeignPtr_ k
      pls <- fromNativeTopicPartitionList' pl
      callback (KafkaConsumer (Kafka k') kc) (KafkaResponseError err) pls

-------------------------------------------------------------------------------
redirectPartitionQueue :: Kafka -> TopicName -> PartitionId -> RdKafkaQueueTPtr -> IO ()
redirectPartitionQueue (Kafka k) (TopicName t) (PartitionId p) q = do
  mpq <- rdKafkaQueueGetPartition k (Text.unpack t) p
  case mpq of
    Nothing -> return ()
    Just pq -> rdKafkaQueueForward pq q

setRebalanceCallback :: (KafkaConsumer -> RebalanceEvent -> IO ())
                          -> KafkaConsumer
                          -> KafkaError
                          -> RdKafkaTopicPartitionListTPtr -> IO ()
setRebalanceCallback f k e pls = do
  ps <- fromNativeTopicPartitionList'' pls
  let assignment = (tpTopicName &&& tpPartition) <$> ps
  case e of
    KafkaResponseError RdKafkaRespErrAssignPartitions -> do
        f k (RebalanceBeforeAssign assignment)
        void $ assign' k pls -- pass as pointer to avoid possible serialisation issues
        mbq <- getRdMsgQueue $ getKafkaConf k
        case mbq of
          Nothing -> pure ()
          Just mq -> do
            forM_ ps (\tp -> redirectPartitionQueue (getKafka k) (tpTopicName tp) (tpPartition tp) mq)
        f k (RebalanceAssign assignment)
    KafkaResponseError RdKafkaRespErrRevokePartitions -> do
        f k (RebalanceBeforeRevoke assignment)
        void $ assign k []
        f k (RebalanceRevoke assignment)
    x -> error $ "Rebalance: UNKNOWN response: " <> show x

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
assign :: KafkaConsumer -> [TopicPartition] -> IO (Maybe KafkaError)
assign (KafkaConsumer (Kafka k) _) ps =
    let pl = if null ps
                then newForeignPtr_ nullPtr
                else toNativeTopicPartitionList ps
        er = KafkaResponseError <$> (pl >>= rdKafkaAssign k)
    in kafkaErrorToMaybe <$> er

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
assign' :: KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
assign' (KafkaConsumer (Kafka k) _) pls =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaAssign k pls

