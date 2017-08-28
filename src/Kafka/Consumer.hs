module Kafka.Consumer
( module X
, runConsumer
, newConsumer
, assign, assignment, subscription
, pollMessage
, commitOffsetMessage, commitAllOffsets, commitPartitionsOffsets
, closeConsumer

-- ReExport Types
, TopicName (..)
, CIT.OffsetCommit (..)
, CIT.PartitionOffset (..)
, CIT.TopicPartition (..)
, CIT.ConsumerRecord (..)
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow
import           Control.Exception
import           Control.Monad          (forM_)
import           Control.Monad.IO.Class
import           Data.Bifunctor
import qualified Data.ByteString        as BS
import qualified Data.Map               as M
import           Foreign                hiding (void)
import           Kafka.Consumer.Convert
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared

import qualified Kafka.Consumer.Types as CIT

import Kafka.Consumer.ConsumerProperties as X
import Kafka.Consumer.Subscription       as X
import Kafka.Consumer.Types              as X
import Kafka.Types                       as X

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
  kc@(KafkaConf kc') <- newConsumerConf cp
  tp' <- topicConf (TopicProps $ M.toList tp)
  _   <- setDefaultTopicConf kc tp'
  rdk <- bimap KafkaError Kafka <$> newRdKafkaT RdKafkaConsumer kc'
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
pollMessage (KafkaConsumer (Kafka k) _) (Timeout ms) =
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

-- | Commit offsets for all currently assigned partitions.
commitPartitionsOffsets :: MonadIO m
                 => OffsetCommit
                 -> KafkaConsumer
                 -> [TopicPartition]
                 -> m (Maybe KafkaError)
commitPartitionsOffsets o k ps =
  liftIO $ toNativeTopicPartitionList ps >>= commitOffsets o k

-- | Assigns specified partitions to a current consumer.
-- Assigning an empty list means unassigning from all partitions that are currently assigned.
assign :: MonadIO m => KafkaConsumer -> [TopicPartition] -> m KafkaError
assign (KafkaConsumer (Kafka k) _) ps =
    let pl = if null ps
                then newForeignPtr_ nullPtr
                else toNativeTopicPartitionList ps
    in  liftIO $ KafkaResponseError <$> (pl >>= rdKafkaAssign k)

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


-- | Closes the consumer.
closeConsumer :: MonadIO m => KafkaConsumer -> m (Maybe KafkaError)
closeConsumer (KafkaConsumer (Kafka k) _) =
  liftIO $ (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaConsumerClose k

-----------------------------------------------------------------------------
newConsumerConf :: ConsumerProperties -> IO KafkaConf
newConsumerConf (ConsumerProperties m _ cbs) = do
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
setDefaultTopicConf (KafkaConf kc) (TopicConf tc) =
    rdKafkaTopicConfDup tc >>= rdKafkaConfSetDefaultTopicConf kc

commitOffsets :: OffsetCommit -> KafkaConsumer -> RdKafkaTopicPartitionListTPtr -> IO (Maybe KafkaError)
commitOffsets o (KafkaConsumer (Kafka k) _) pl =
    (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaCommit k pl (offsetCommitToBool o)

setConsumerLogLevel :: KafkaConsumer -> KafkaLogLevel -> IO ()
setConsumerLogLevel (KafkaConsumer (Kafka k) _) level =
  liftIO $ rdKafkaSetLogLevel k (fromEnum level)

redirectCallbacksPoll :: KafkaConsumer -> IO (Maybe KafkaError)
redirectCallbacksPoll (KafkaConsumer (Kafka k) _) =
  (kafkaErrorToMaybe . KafkaResponseError) <$> rdKafkaPollSetConsumer k
