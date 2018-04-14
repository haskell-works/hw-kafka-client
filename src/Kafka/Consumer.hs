{-# LANGUAGE TupleSections #-}
module Kafka.Consumer
( module X
, runConsumer
, newConsumer
, assignment, subscription
, pausePartitions, resumePartitions
, committed, position, seek
, pollMessage, pollConsumerEvents
, commitOffsetMessage, commitAllOffsets, commitPartitionsOffsets
, storeOffsetMessage
, closeConsumer
-- ReExport Types
, KafkaConsumer
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow
import           Control.Concurrent               (forkIO, rtsSupportsBoundThreads)
import           Control.Exception
import           Control.Monad                    (forM_, void, when)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except
import           Data.Bifunctor
import qualified Data.ByteString                  as BS
import           Data.IORef
import qualified Data.Map                         as M
import           Data.Maybe                       (fromMaybe)
import           Data.Monoid                      ((<>))
import           Foreign                          hiding (void)
import           Kafka.Consumer.Convert
import           Kafka.Consumer.Types
import           Kafka.Internal.CancellationToken as CToken
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared

import Kafka.Consumer.ConsumerProperties as X
import Kafka.Consumer.Subscription       as X
import Kafka.Consumer.Types              as X hiding (KafkaConsumer)
import Kafka.Types                       as X

-- | Runs high-level kafka consumer.
-- A callback provided is expected to call 'pollMessage' when convenient.
{-# DEPRECATED runConsumer "Use newConsumer/closeConsumer instead" #-}
runConsumer :: ConsumerProperties
            -> Subscription
            -> (KafkaConsumer -> IO (Either KafkaError a))  -- ^ A callback function to poll and handle messages
            -> IO (Either KafkaError a)
runConsumer cp sub f =
  bracket mkConsumer clConsumer runHandler
  where
    mkConsumer = newConsumer cp sub

    clConsumer (Left err) = return (Left err)
    clConsumer (Right kc) = maybeToLeft <$> closeConsumer kc

    runHandler (Left err) = return (Left err)
    runHandler (Right kc) = f kc

newConsumer :: MonadIO m
            => ConsumerProperties
            -> Subscription
            -> m (Either KafkaError KafkaConsumer)
newConsumer props (Subscription ts tp) = liftIO $ do
  let cp = setCallback (rebalanceCallback (\_ _ -> return ())) <> props
  kc@(KafkaConf kc' qref ct) <- newConsumerConf cp
  tp' <- topicConf (TopicProps $ M.toList tp)
  _   <- setDefaultTopicConf kc tp'
  rdk <- newRdKafkaT RdKafkaConsumer kc'
  case rdk of
    Left err   -> return . Left $ KafkaError err
    Right rdk' -> do
      msgq <- rdKafkaQueueNew rdk'
      writeIORef qref (Just msgq)
      let kafka = KafkaConsumer (Kafka rdk') kc
      redErr <- redirectCallbacksPoll kafka
      case redErr of
        Just err -> closeConsumer kafka >> return (Left err)
        Nothing  -> do
          forM_ (cpLogLevel cp) (setConsumerLogLevel kafka)
          sub <- subscribe kafka ts
          case sub of
            Nothing  -> runConsumerLoop kafka ct (Just $ Timeout 100) >> return (Right kafka)
            Just err -> closeConsumer kafka >> return (Left err)

pollMessage :: MonadIO m
            => KafkaConsumer
            -> Timeout -- ^ the timeout, in milliseconds
            -> m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))) -- ^ Left on error or timeout, right for success
pollMessage c@(KafkaConsumer _ (KafkaConf _ qr _)) (Timeout ms) = liftIO $ do
    pollConsumerEvents c Nothing
    mbq <- readIORef qr
    case mbq of
      Nothing -> return . Left $ KafkaBadSpecification "Messages queue is not configured, internal error, fatal."
      Just q  -> rdKafkaConsumeQueue q (fromIntegral ms) >>= fromMessagePtr

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: MonadIO m
                    => OffsetCommit
                    -> KafkaConsumer
                    -> ConsumerRecord k v
                    -> m (Maybe KafkaError)
commitOffsetMessage o k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessageForCommit m] >>= commitOffsets o k

-- | Stores message's offset locally for the message's partition.
storeOffsetMessage :: MonadIO m
                    => KafkaConsumer
                    -> ConsumerRecord k v
                    -> m (Maybe KafkaError)
storeOffsetMessage k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessageForCommit m] >>= commitOffsetsStore k

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> m (Maybe KafkaError)
commitAllOffsets o k =
  liftIO $ newForeignPtr_ nullPtr >>= commitOffsets o k

-- | Commit offsets for all currently assigned partitions.
commitPartitionsOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> [TopicPartition]
                 -> m (Maybe KafkaError)
commitPartitionsOffsets o k ps =
  liftIO $ toNativeTopicPartitionList ps >>= commitOffsets o k

-- | Returns current consumer's assignment
assignment :: MonadIO m => KafkaConsumer -> m (Either KafkaError (M.Map TopicName [PartitionId]))
assignment (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaAssignment k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ tpMap <$> tps
  where
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts

-- | Returns current consumer's subscription
subscription :: MonadIO m => KafkaConsumer -> m (Either KafkaError [(TopicName, SubscribedPartitions)])
subscription (KafkaConsumer (Kafka k) _) = liftIO $ do
  tpl <- rdKafkaSubscription k
  tps <- traverse fromNativeTopicPartitionList'' (left KafkaResponseError tpl)
  return $ toSub <$> tps
  where
    toSub ts = M.toList $ subParts <$> tpMap ts
    tpMap ts = toMap $ (tpTopicName &&& tpPartition) <$> ts
    subParts [PartitionId (-1)] = SubscribedPartitionsAll
    subParts ps                 = SubscribedPartitions ps

-- | Pauses specified partitions on the current consumer.
pausePartitions :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
pausePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl topicName partitionId) ps
  KafkaResponseError <$> rdKafkaPausePartitions k pl

-- | Resumes specified partitions on the current consumer.
resumePartitions :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m KafkaError
resumePartitions (KafkaConsumer (Kafka k) _) ps = liftIO $ do
  pl <- newRdKafkaTopicPartitionListT (length ps)
  mapM_ (\(TopicName topicName, PartitionId partitionId) -> rdKafkaTopicPartitionListAdd pl topicName partitionId) ps
  KafkaResponseError <$> rdKafkaResumePartitions k pl

seek :: MonadIO m => KafkaConsumer -> Timeout -> [TopicPartition] -> m (Maybe KafkaError)
seek (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $
  either Just (const Nothing) <$> seekAll
  where
    seekAll = runExceptT $ do
      tr <- traverse (ExceptT . topicPair) tps
      _  <- traverse (\(kt, p, o) -> ExceptT (rdSeek kt p o)) tr
      return ()

    rdSeek kt (PartitionId p) o = do
      res <- rdKafkaSeek kt (fromIntegral p) (offsetToInt64 o) timeout
      return $ rdKafkaErrorToEither res

    topicPair tp = do
      let (TopicName tn) = tpTopicName tp
      nt <- newRdKafkaTopicT k tn Nothing
      return $ bimap KafkaError (,tpPartition tp, tpOffset tp) nt

-- | Retrieve committed offsets for topics+partitions.
committed :: MonadIO m => KafkaConsumer -> Timeout -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
committed (KafkaConsumer (Kafka k) _) (Timeout timeout) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaCommitted k ntps timeout
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err                   -> return $ Left (KafkaResponseError err)

-- | Retrieve current positions (last consumed message offset+1) for the current running instance of the consumer.
-- If the current consumer hasn't received any messages for a given partition, 'PartitionOffsetInvalid' is returned.
position :: MonadIO m => KafkaConsumer -> [(TopicName, PartitionId)] -> m (Either KafkaError [TopicPartition])
position (KafkaConsumer (Kafka k) _) tps = liftIO $ do
  ntps <- toNativeTopicPartitionList' tps
  res <- rdKafkaPosition k ntps
  case res of
    RdKafkaRespErrNoError -> Right <$> fromNativeTopicPartitionList'' ntps
    err                   -> return $ Left (KafkaResponseError err)

-- | Polls the provided kafka consumer for events.
--
-- Events will cause application provided callbacks to be called.
--
-- The \p timeout_ms argument specifies the maximum amount of time
-- (in milliseconds) that the call will block waiting for events.
--
-- This function is called on each 'pollMessage' and, if runtime allows
-- multi threading, it is called periodically in a separate thread
-- to ensure the callbacks are handled ASAP.
--
-- There is no particular need to call this function manually
-- unless some special cases in a single-threaded environment
-- when polling for events on each 'pollMessage' is not
-- frequent enough.
pollConsumerEvents :: KafkaConsumer -> Maybe Timeout -> IO ()
pollConsumerEvents k timeout =
  let (Timeout tm) = fromMaybe (Timeout 0) timeout
  in void $ rdKafkaConsumerPoll (getRdKafka k) tm

-- | Closes the consumer.
closeConsumer :: MonadIO m => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer (Kafka k) (KafkaConf _ qr ct)) = liftIO $ do
  CToken.cancel ct
  mbq <- readIORef qr
  void $ traverse rdKafkaQueueDestroy mbq
  (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaConsumerClose k

-----------------------------------------------------------------------------
newConsumerConf :: ConsumerProperties -> IO KafkaConf
newConsumerConf ConsumerProperties {cpProps = m, cpCallbacks = cbs} = do
  conf <- kafkaConf (KafkaProps $ M.toList m)
  forM_ cbs (\setCb -> setCb conf)
  return conf

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: KafkaConsumer -> [TopicName] -> IO (Maybe KafkaError)
subscribe (KafkaConsumer (Kafka k) _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl t (-1)) ts
    res <- KafkaResponseError <$> rdKafkaSubscribe k pl
    return $ kafkaErrorToMaybe res

setDefaultTopicConf :: KafkaConf -> TopicConf -> IO ()
setDefaultTopicConf (KafkaConf kc _ _) (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer (Kafka k) _) pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)

commitOffsetsStore :: KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsetsStore (KafkaConsumer (Kafka k) _) pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaOffsetsStore k pl

setConsumerLogLevel :: KafkaConsumer -> KafkaLogLevel -> IO ()
setConsumerLogLevel (KafkaConsumer (Kafka k) _) level =
  liftIO $ rdKafkaSetLogLevel k (fromEnum level)

redirectCallbacksPoll :: KafkaConsumer -> IO (Maybe KafkaError)
redirectCallbacksPoll (KafkaConsumer (Kafka k) _) =
  (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaPollSetConsumer k

runConsumerLoop :: KafkaConsumer -> CancellationToken -> Maybe Timeout -> IO ()
runConsumerLoop k ct timeout =
  when rtsSupportsBoundThreads $ void $ forkIO go
  where
    go = do
      token <- CToken.status ct
      case token of
        Running   -> pollConsumerEvents k timeout >> go
        Cancelled -> return ()

