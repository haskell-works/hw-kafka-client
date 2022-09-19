-----------------------------------------------------------------------------
-- |
-- Module to work wih Kafkas transactional producers.
-- 
-----------------------------------------------------------------------------
module Kafka.Transaction
( initTransactions
, beginTransaction
, commitTransaction
, abortTransaction

, commitOffsetMessageTransaction
-- , commitTransactionWithOffsets

, TxError
, getKafkaError
, kafkaErrorIsFatal
, kafkaErrorIsRetriable
, kafkaErrorTxnRequiresAbort
)
where

import           Control.Monad.IO.Class   (MonadIO (liftIO))
import           Kafka.Internal.RdKafka   (RdKafkaErrorTPtr, rdKafkaErrorDestroy, rdKafkaErrorIsFatal, rdKafkaErrorIsRetriable, rdKafkaErrorTxnRequiresAbort, rdKafkaErrorCode, rdKafkaInitTransactions, rdKafkaBeginTransaction, rdKafkaCommitTransaction, rdKafkaAbortTransaction, rdKafkaSendOffsetsToTransaction)
import           Kafka.Internal.Setup     (getRdKafka)
import           Kafka.Producer.Convert   (handleProduceErrT)
import           Kafka.Producer
import           Kafka.Consumer.Convert   (toNativeTopicPartitionList, topicPartitionFromMessageForCommit)
import           Kafka.Consumer

-------------------------------------------------------------------------------------
-- Tx API

data TxError = TxError 
  { txErrorKafka       :: !KafkaError 
  , txErrorFatal       :: !Bool
  , txErrorRetriable   :: !Bool
  , txErrorTxnReqAbort :: !Bool
  } 

-- | Initialises Kafka for transactions 
initTransactions :: MonadIO m 
                 => KafkaProducer 
                 -> Timeout 
                 -> m (Maybe KafkaError)
initTransactions p (Timeout to) 
  = liftIO $ rdKafkaInitTransactions (getRdKafka p) to >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Begins a new transaction
beginTransaction :: MonadIO m 
                 => KafkaProducer 
                 -> m (Maybe KafkaError)
beginTransaction p 
  = liftIO $ rdKafkaBeginTransaction (getRdKafka p) >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Commits an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitTransaction :: MonadIO m 
                  => KafkaProducer 
                  -> Timeout 
                  -> m (Maybe TxError)
commitTransaction p (Timeout to) = liftIO $ rdKafkaCommitTransaction (getRdKafka p) to >>= toTxError

-- | Aborts an existing transaction
-- Pre-condition: there exists an open transaction, created with beginTransaction
abortTransaction :: MonadIO m 
                 => KafkaProducer 
                 -> Timeout 
                 -> m (Maybe KafkaError)
abortTransaction p (Timeout to) 
  = liftIO $ do rdKafkaAbortTransaction (getRdKafka p) to >>= rdKafkaErrorCode >>= handleProduceErrT

-- | Commits the message's offset in the current transaction
--    Similar to Kafka.Consumer.commitOffsetMessage but within a transactional context
-- Pre-condition: there exists an open transaction, created with beginTransaction
commitOffsetMessageTransaction :: MonadIO m 
                               => KafkaProducer 
                               -> KafkaConsumer 
                               -> ConsumerRecord k v
                               -> Timeout 
                               -> m (Maybe TxError)
commitOffsetMessageTransaction p c m (Timeout to) = liftIO $ do
  tps <- toNativeTopicPartitionList [topicPartitionFromMessageForCommit m]
  rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError

-- -- | Commit offsets for all currently assigned partitions in the current transaction
-- --    Similar to Kafka.Consumer.commitAllOffsets but within a transactional context
-- -- Pre-condition: there exists an open transaction, created with beginTransaction
-- commitAllOffsetsTransaction :: MonadIO m 
--                             => KafkaProducer 
--                             -> KafkaConsumer 
--                             -> Timeout 
--                             -> m (Maybe TxError)
-- commitAllOffsetsTransaction p c (Timeout to) = liftIO $ do
--   -- TODO: this can't be right...
--   tps <- newForeignPtr_ nullPtr
--   rdKafkaSendOffsetsToTransaction (getRdKafka p) (getRdKafka c) tps to >>= toTxError
    
getKafkaError :: TxError -> KafkaError
getKafkaError = txErrorKafka

kafkaErrorIsFatal :: TxError -> Bool
kafkaErrorIsFatal = txErrorFatal

kafkaErrorIsRetriable :: TxError -> Bool
kafkaErrorIsRetriable = txErrorRetriable

kafkaErrorTxnRequiresAbort :: TxError -> Bool
kafkaErrorTxnRequiresAbort = txErrorTxnReqAbort

----------------------------------------------------------------------------------------------------
-- Implementation detail, used internally
toTxError :: RdKafkaErrorTPtr -> IO (Maybe TxError)
toTxError errPtr = do
  ret <- rdKafkaErrorCode errPtr >>= handleProduceErrT
  case ret of
    Nothing -> do
      -- NOTE: don't forget to free error structure, otherwise we are leaking memory!
      rdKafkaErrorDestroy errPtr
      pure Nothing
    Just ke -> do
      fatal     <- rdKafkaErrorIsFatal errPtr
      retriable <- rdKafkaErrorIsRetriable errPtr
      reqAbort  <- rdKafkaErrorTxnRequiresAbort errPtr
      -- NOTE: don't forget to free error structure, otherwise we are leaking memory!
      rdKafkaErrorDestroy errPtr
      pure $ Just $ TxError 
        { txErrorKafka       = ke
        , txErrorFatal       = fatal
        , txErrorRetriable   = retriable
        , txErrorTxnReqAbort = reqAbort
        }
