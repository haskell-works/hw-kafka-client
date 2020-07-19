{-# LANGUAGE BangPatterns #-}
module Kafka.Consumer.Callbacks
( rebalanceCallback
, offsetCommitCallback
, module X
)
where

import Control.Arrow          ((&&&))
import Control.Monad          (forM_, void)
import Foreign.ForeignPtr     (newForeignPtr_)
import Foreign.Ptr            (nullPtr)
import Kafka.Callbacks        as X
import Kafka.Consumer.Convert (fromNativeTopicPartitionList', fromNativeTopicPartitionList'')
import Kafka.Consumer.Types   (KafkaConsumer (..), RebalanceEvent (..), TopicPartition (..))
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup   (HasKafka (..), HasKafkaConf (..), Kafka (..), KafkaConf (..), getRdMsgQueue)
import Kafka.Types            (KafkaError (..), PartitionId (..), TopicName (..))

import qualified Data.Text as Text

-- | Sets a callback that is called when rebalance is needed.
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
-- for this callback and is served by 'Kafka.Consumer.pollMessage'.
--
-- If no partitions had valid offsets to commit this callback will be called
-- with 'KafkaResponseError' 'RdKafkaRespErrNoOffset' which is not to be considered
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
  let (Kafka kptr) = getKafka k

  case e of
    KafkaResponseError RdKafkaRespErrAssignPartitions -> do
        f k (RebalanceBeforeAssign assignment)
        void $ rdKafkaAssign kptr pls

        mbq <- getRdMsgQueue $ getKafkaConf k
        case mbq of
          Nothing -> pure ()
          Just mq -> do
            {- Magnus Edenhill:
                If you redirect after assign() it means some messages may be forwarded to the single consumer queue,
                so either do it before assign() or do: assign(); pause(); redirect; resume()
            -}
            void $ rdKafkaPausePartitions kptr pls
            forM_ ps (\tp -> redirectPartitionQueue (getKafka k) (tpTopicName tp) (tpPartition tp) mq)
            void $ rdKafkaResumePartitions kptr pls

        f k (RebalanceAssign assignment)

    KafkaResponseError RdKafkaRespErrRevokePartitions -> do
        f k (RebalanceBeforeRevoke assignment)
        void $ newForeignPtr_ nullPtr >>= rdKafkaAssign kptr
        f k (RebalanceRevoke assignment)
    x -> error $ "Rebalance: UNKNOWN response: " <> show x
