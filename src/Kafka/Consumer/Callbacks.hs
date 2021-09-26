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
import Kafka.Consumer.Convert (fromNativeTopicPartitionList', fromNativeTopicPartitionList'', toNativeTopicPartitionList)
import Kafka.Consumer.Types   (KafkaConsumer (..), RebalanceEvent (..), TopicPartition (..))
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup   (HasKafka (..), HasKafkaConf (..), Kafka (..), KafkaConf (..), getRdMsgQueue, Callback (..))
import Kafka.Types            (KafkaError (..), PartitionId (..), TopicName (..))

import qualified Data.Text as Text

-- | Sets a callback that is called when rebalance is happening.
--
-- If you want to store the offsets locally, return the TopicPartition list with
-- modified offsets from RebalanceBeforeAssign. Callback return value on other
-- events is ignored.
rebalanceCallback :: (KafkaConsumer -> RebalanceEvent -> IO (Maybe [TopicPartition])) -> Callback
rebalanceCallback callback =
  Callback $ \kc@(KafkaConf con _ _) -> rdKafkaConfSetRebalanceCb con (realCb kc)
  where
    realCb kc k err pl = do
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
offsetCommitCallback :: (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ()) -> Callback
offsetCommitCallback callback =
  Callback $ \kc@(KafkaConf conf _ _) -> rdKafkaConfSetOffsetCommitCb conf (realCb kc)
  where
    realCb kc k err pl = do
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

setRebalanceCallback :: (KafkaConsumer -> RebalanceEvent -> IO (Maybe [TopicPartition]))
                          -> KafkaConsumer
                          -> KafkaError
                          -> RdKafkaTopicPartitionListTPtr -> IO ()
setRebalanceCallback f k e pls = do
  ps <- fromNativeTopicPartitionList'' pls
  let (Kafka kptr) = getKafka k

  case e of
    KafkaResponseError RdKafkaRespErrAssignPartitions -> do
        -- Consumer may want to alter the offsets if they are stored locally.
        mbAltered <- f k (RebalanceBeforeAssign ps)
        (pls', assigned) <- case mbAltered of
          Nothing -> pure (pls, ps)
          Just alt -> do
            pls' <- toNativeTopicPartitionList alt
            pure (pls', alt)
        void $ rdKafkaAssign kptr pls'

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

        void $ f k (RebalanceAssign assigned)

    KafkaResponseError RdKafkaRespErrRevokePartitions -> do
        void $ f k (RebalanceBeforeRevoke ps)
        void $ newForeignPtr_ nullPtr >>= rdKafkaAssign kptr
        void $ f k (RebalanceRevoke ps)
    x -> error $ "Rebalance: UNKNOWN response: " <> show x
