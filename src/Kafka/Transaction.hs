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

, commitOffsetMessageTransaction
, commitAllOffsetsTransaction

, TxError
, getKafkaError
, kafkaErrorIsFatal
, kafkaErrorIsRetriable
, kafkaErrorTxnRequiresAbort
)
where

import           Control.Monad.IO.Class   (MonadIO (liftIO))
import           Foreign                  hiding (void)
import           Kafka.Internal.RdKafka   (RdKafkaErrorTPtr, rdKafkaErrorIsFatal, rdKafkaErrorIsRetriable, rdKafkaErrorTxnRequiresAbort, rdKafkaErrorCode, rdKafkaInitTransactions, rdKafkaBeginTransaction, rdKafkaCommitTransaction, rdKafkaAbortTransaction, rdKafkaSendOffsetsToTransaction)
import           Kafka.Internal.Setup     (getRdKafka)
import           Kafka.Producer.Convert   (handleProduceErrT)
import           Kafka.Producer
import           Kafka.Consumer.Convert   (toNativeTopicPartitionList, topicPartitionFromMessageForCommit)
import           Kafka.Consumer
import GHC.IO (unsafePerformIO)

type InternalError = RdKafkaErrorTPtr
data TxError = TxError !KafkaError !InternalError

-- | Initialises Kafka for transactions 
initTransactions :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe TxError)
initTransactions p (Timeout to) = liftIO $ rdKafkaInitTransactions (getRdKafka p) to >>= toTxError

-- | Begins a new transaction
beginTransaction :: MonadIO m => KafkaProducer -> m (Maybe TxError)
beginTransaction p = liftIO $ rdKafkaBeginTransaction (getRdKafka p) >>= toTxError

-- | Commits an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitTransaction :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe TxError)
commitTransaction p (Timeout to) = liftIO $ rdKafkaCommitTransaction (getRdKafka p) to >>= toTxError

-- | Aborts an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
abortTransaction :: MonadIO m => KafkaProducer -> Timeout -> m (Maybe TxError)
abortTransaction p (Timeout to) = liftIO $ do rdKafkaAbortTransaction (getRdKafka p) to >>= toTxError

-- | Commits the message's offset in the current transaction
--    Similar to Kafka.Consumer.commitOffsetMessage but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitOffsetMessageTransaction :: MonadIO m => KafkaProducer -> KafkaConsumer -> ConsumerRecord k v -> Timeout -> m (Maybe TxError)
commitOffsetMessageTransaction p c m (Timeout to) = liftIO $ do
  tps <- toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

-- | Commit offsets for all currently assigned partitions in the current transaction
--    Similar to Kafka.Consumer.commitAllOffsets but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitAllOffsetsTransaction :: MonadIO m => KafkaProducer -> KafkaConsumer -> Timeout -> m (Maybe TxError)
commitAllOffsetsTransaction p c (Timeout to) = liftIO $ do
  tps <- newForeignPtr_ nullPtr
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

getKafkaError :: TxError -> KafkaError
getKafkaError (TxError kErr _) = kErr

kafkaErrorIsFatal :: TxError -> Bool
kafkaErrorIsFatal (TxError _ internalErr) = unsafePerformIO $ rdKafkaErrorIsFatal internalErr

kafkaErrorIsRetriable :: TxError -> Bool
kafkaErrorIsRetriable (TxError _ internalErr) = unsafePerformIO $ rdKafkaErrorIsRetriable internalErr

kafkaErrorTxnRequiresAbort :: TxError -> Bool
kafkaErrorTxnRequiresAbort (TxError _ internalErr) = unsafePerformIO $ rdKafkaErrorTxnRequiresAbort internalErr

----------------------------------------------------------------------------------------------------
-- Implementation detail, used internarlly
toTxError :: RdKafkaErrorTPtr -> IO (Maybe TxError)
toTxError errPtr = do
  ret  <- rdKafkaErrorCode errPtr >>= handleProduceErrT
  case ret of
    Nothing -> pure Nothing
    Just ke -> pure $ Just $ TxError ke errPtr 
