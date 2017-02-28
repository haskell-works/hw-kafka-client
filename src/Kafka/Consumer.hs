{-# LANGUAGE TupleSections #-}
module Kafka.Consumer
( module X
, runConsumer
, newConsumer
, assign
, pollMessage
, commitOffsetMessage
, commitAllOffsets
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
import           Control.Monad (forM_)
import           Control.Monad.IO.Class
import qualified Data.ByteString as BS
import qualified Data.Map as M
import           Foreign                         hiding (void)
import           Kafka.Consumer.Convert
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared

import qualified Kafka.Consumer.Types   as CIT
import qualified Kafka.Internal.RdKafkaEnum      as RDE

import Kafka.Types as X
import Kafka.Consumer.Types as X
import Kafka.Consumer.Subscription as X
import Kafka.Consumer.ConsumerProperties as X

-- | Runs high-level kafka consumer.
--
-- A callback provided is expected to call 'pollMessage' when convenient.
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

-- | Creates a kafka consumer.
-- A new consumer MUST be closed with 'closeConsumer' function.
newConsumer :: MonadIO m
            => ConsumerProperties
            -> Subscription
            -> m (Either KafkaError KafkaConsumer)
newConsumer cp (Subscription ts tp) = liftIO $ do
  (KafkaConf kc) <- newConsumerConf cp
  tp' <- topicConf (TopicProps $ M.toList tp)
  _   <- setDefaultTopicConf kc tp'
  rdk <- mapLeft KafkaError <$> newRdKafkaT RdKafkaConsumer kc
  case flip KafkaConsumer kc <$> rdk of
    Left err -> return $ Left err
    Right kafka -> do
      redErr <- redirectCallbacksPoll kafka
      case redErr of
        Just err -> closeConsumer kafka >> return (Left err)
        Nothing -> do
          forM_ (cpLogLevel cp) (setConsumerLogLevel kafka)
          sub <- subscribe kafka ts
          case sub of
            Nothing  -> return $ Right kafka
            Just err -> closeConsumer kafka >> return (Left err)


-- | Polls the next message from a subscription
pollMessage :: MonadIO m
            => KafkaConsumer
            -> Timeout -- ^ the timeout, in milliseconds
            -> m (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString))) -- ^ Left on error or timeout, right for success
pollMessage (KafkaConsumer k _) (Timeout ms) =
    liftIO $ rdKafkaConsumerPoll k (fromIntegral ms) >>= fromMessagePtr

-- | Commit message's offset on broker for the message's partition.
commitOffsetMessage :: MonadIO m
                    => OffsetCommit
                    -> KafkaConsumer
                    -> ConsumerRecord k v
                    -> m (Maybe KafkaError)
commitOffsetMessage o k m =
  liftIO $ toNativeTopicPartitionList [topicPartitionFromMessage m] >>= commitOffsets o k

-- | Commit offsets for all currently assigned partitions.
commitAllOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> m (Maybe KafkaError)
commitAllOffsets o k =
  liftIO $ newForeignPtr_ nullPtr >>= commitOffsets o k

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
assign :: MonadIO m => KafkaConsumer -> [TopicPartition] -> m KafkaError
assign (KafkaConsumer k _) ps =
    let pl = if null ps
                then newForeignPtr_ nullPtr
                else toNativeTopicPartitionList ps
    in  liftIO $ KafkaResponseError <$> (pl >>= rdKafkaAssign k)


-- | Closes the consumer.
closeConsumer :: MonadIO m => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer k _) =
  liftIO $ (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaConsumerClose k

-----------------------------------------------------------------------------
newConsumerConf :: ConsumerProperties -> IO KafkaConf
newConsumerConf (ConsumerProperties m rcb ccb _) = do
  conf <- kafkaConf (KafkaProps $ M.toList m)
  forM_ rcb (\(ReballanceCallback cb) -> setRebalanceCallback conf cb)
  forM_ ccb (\(OffsetsCommitCallback cb) -> setOffsetCommitCallback conf cb)
  return conf

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
                     -> (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ())
                     -> IO ()
setRebalanceCallback (KafkaConf conf) callback = rdKafkaConfSetRebalanceCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        ps  <- fromNativeTopicPartitionList' pl
        callback (KafkaConsumer rk' conf) (KafkaResponseError err) ps

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
                        -> (KafkaConsumer -> KafkaError -> [TopicPartition] -> IO ())
                        -> IO ()
setOffsetCommitCallback (KafkaConf conf) callback = rdKafkaConfSetOffsetCommitCb conf realCb
  where
    realCb :: Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
    realCb rk err pl _ = do
        rk' <- newForeignPtr_ rk
        ps  <- fromNativeTopicPartitionList' pl
        callback (KafkaConsumer rk' conf) (KafkaResponseError err) ps

-- | Subscribes to a given list of topics.
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
subscribe :: KafkaConsumer -> [TopicName] -> IO (Maybe KafkaError)
subscribe (KafkaConsumer k _) ts = do
    pl <- newRdKafkaTopicPartitionListT (length ts)
    mapM_ (\(TopicName t) -> rdKafkaTopicPartitionListAdd pl t (-1)) ts
    res <- KafkaResponseError <$> rdKafkaSubscribe k pl
    return $ kafkaErrorToMaybe res

setDefaultTopicConf :: RdKafkaConfTPtr -> TopicConf -> IO ()
setDefaultTopicConf kc (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer k _) pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)

setConsumerLogLevel :: KafkaConsumer -> KafkaLogLevel -> IO ()
setConsumerLogLevel (KafkaConsumer k _) level =
  liftIO $ rdKafkaSetLogLevel k (fromEnum level)

redirectCallbacksPoll :: KafkaConsumer -> IO (Maybe KafkaError)
redirectCallbacksPoll (KafkaConsumer k _) =
  (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaPollSetConsumer k
