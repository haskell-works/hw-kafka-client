-----------------------------------------------------------------------------
-- |
-- TODO
-- 
-----------------------------------------------------------------------------
module Kafka.Transaction
( initTransactions
, beginTransaction
, commitTransaction
, abortTransaction
, sendOffsetsToTransaction
)
where

import           Control.Monad.IO.Class   (MonadIO (liftIO))
import           Kafka.Internal.RdKafka   (rdKafkaErrorCode, rdKafkaInitTransactions, rdKafkaBeginTransaction, rdKafkaCommitTransaction, rdKafkaAbortTransaction, rdKafkaSendOffsetsToTransaction)
import           Kafka.Internal.Setup     (getRdKafka)
import           Kafka.Producer.Convert   (handleProduceErrT)
import           Kafka.Producer
import           Kafka.Consumer.Convert   (toNativeTopicPartitionList)
import           Kafka.Consumer

-- | Initialises Kafka for transactions 
initTransactions :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe KafkaError)
initTransactions p (Timeout to) = liftIO $ do
  rdKafkaInitTransactions (getRdKafka p) to >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Begins a new transaction
beginTransaction :: MonadIO m => KafkaProducer -> m (Maybe KafkaError)
beginTransaction p = liftIO $ do
  rdKafkaBeginTransaction (getRdKafka p) >>= rdKafkaErrorCode >>= handleProduceErrT

sendOffsetsToTransaction :: MonadIO m => KafkaProducer -> KafkaConsumer -> [TopicPartition] -> Timeout -> m (Maybe KafkaError)
sendOffsetsToTransaction p c ps (Timeout to) = liftIO $ do
  tps <- toNativeTopicPartitionList ps
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Commits an existing transaction
commitTransaction :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe KafkaError)
commitTransaction p (Timeout to) = liftIO $ do
  rdKafkaCommitTransaction (getRdKafka p) to >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Aborts an existing transaction
abortTransaction :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe KafkaError)
abortTransaction p (Timeout to) = liftIO $ do
  rdKafkaAbortTransaction (getRdKafka p) to >>= rdKafkaErrorCode >>= handleProduceErrT
