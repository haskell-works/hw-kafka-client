{-# LANGUAGE RecordWildCards #-}
module Kafka.Admin
( module X
, NewTopic (..)
, ReplicationFactor(..)
, PartitionsCount(..)
, KafkaAdmin

, newKafkaAdmin
, createTopics
, deleteTopics
, closeKafkaAdmin
)
where

import           Control.Exception          (bracket, displayException)
import           Control.Monad              (void)
import           Control.Monad.IO.Class     (MonadIO, liftIO)
import           Control.Monad.Trans.Class  (lift)
import           Control.Monad.Trans.Except (ExceptT (..), runExceptT, withExceptT)
import           Control.Monad.Trans.Maybe  (MaybeT (..), runMaybeT)
import           Data.Bifunctor             (bimap, first)
import           Data.Either                (partitionEithers)
import           Data.Foldable              (traverse_)
import           Data.List.NonEmpty         (NonEmpty (..))
import qualified Data.List.NonEmpty         as NEL
import qualified Data.Map                   as M
import           Data.Maybe                 (fromMaybe)
import           Data.Semigroup             ((<>))
import qualified Data.Set                   as S
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Types

import Kafka.Admin.AdminProperties as X

newtype ReplicationFactor = ReplicationFactor { unReplicationFactor :: Int } deriving (Show, Eq, Read, Ord)
newtype PartitionsCount = PartitionsCount { unPartitionsCount :: Int } deriving (Show, Eq, Read, Ord)

{-# DEPRECATED KafkaAdmin "Do we even need a special KafkaAdmin now when all the functions accept HasKafka?" #-}
data KafkaAdmin = KafkaAdmin
  { acKafka     :: !Kafka
  , acKafkaConf :: !KafkaConf
  }
instance HasKafka KafkaAdmin where
  getKafka = acKafka

data NewTopic = NewTopic
  { ntName              :: TopicName
  , ntPartitions        :: PartitionsCount
  , ntReplicationFactor :: ReplicationFactor
  , ntConfig            :: M.Map Text Text
  } deriving (Show)

{-# DEPRECATED newKafkaAdmin "Do we even need a special KafkaAdmin now when all the functions accept HasKafka?" #-}
newKafkaAdmin :: (MonadIO m)
               => AdminProperties
               -> m (Either KafkaError KafkaAdmin)
newKafkaAdmin props = liftIO $ do
  kc@(KafkaConf kc' _ _) <- kafkaConf (KafkaProps $ apKafkaProps props)
  mbKafka <- newRdKafkaT RdKafkaConsumer kc'
  case mbKafka of
    Left err    -> pure . Left $ KafkaError err
    Right kafka -> pure $ Right $ KafkaAdmin (Kafka kafka) kc

closeKafkaAdmin :: KafkaAdmin -> IO ()
closeKafkaAdmin k = void $ rdKafkaConsumerClose (getRdKafka k)

----------------------------- CREATE ------------------------------------------

createTopics :: (HasKafka k)
             => k
             -> [NewTopic]
             -> IO [Either (KafkaError, Text) TopicName]
createTopics client ts =
  withAdminOperation client $ \(kafkaPtr, opts, queue) -> do
    let topicNames = ntName <$> ts
    crRes <- withNewTopics ts $ \topics ->
              rdKafkaCreateTopics kafkaPtr topics opts queue
    case crRes of
      Left es -> pure $ (\e -> Left (e, Text.pack $ displayException e)) <$> NEL.toList es
      Right _ -> do
        res <- waitForAllResponses topicNames rdKafkaEventCreateTopicsResult rdKafkaCreateTopicsResultTopics queue
        pure $ first (\(_, a, b) -> (a, b)) <$> res

withNewTopics :: [NewTopic] -> ([RdKafkaNewTopicTPtr] ->  IO a) -> IO (Either (NonEmpty KafkaError) a)
withNewTopics ts =
  withUnsafe ts mkNewTopicUnsafe rdKafkaNewTopicDestroyArray

mkNewTopicUnsafe :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
mkNewTopicUnsafe NewTopic{..} = runExceptT $ do
  t <- withStrErr $ newRdKafkaNewTopicUnsafe (unTopicName ntName) (unPartitionsCount ntPartitions) (unReplicationFactor ntReplicationFactor)
  _ <- withKafkaErr $ whileRight (uncurry $ rdKafkaNewTopicSetConfig undefined) (bimap Text.unpack Text.unpack <$> M.toList ntConfig)
  pure t
  where
    withStrErr   = withExceptT KafkaError . ExceptT
    withKafkaErr = withExceptT KafkaResponseError . ExceptT


----------------------------- DELETE ------------------------------------------

deleteTopics :: (HasKafka k)
             => k
             -> [TopicName]
             -> IO [Either (TopicName, KafkaError, Text) TopicName]
deleteTopics client ts =
  withAdminOperation client $ \(kafkaPtr, opts, queue) -> do
    topics <- traverse (newRdKafkaDeleteTopic . Text.unpack . unTopicName) ts
    rdKafkaDeleteTopics kafkaPtr topics opts queue
    waitForAllResponses ts rdKafkaEventDeleteTopicsResult rdKafkaDeleteTopicsResultTopics queue

-------------------- Hepler servicing functions

withAdminOperation :: HasKafka k
                   => k
                   -> ((RdKafkaTPtr, RdKafkaAdminOptionsTPtr, RdKafkaQueueTPtr) -> IO a)
                   -> IO a
withAdminOperation k f = do
  let kafkaPtr = getRdKafka k
  queue <- newRdKafkaQueue kafkaPtr
  opts <- newRdKafkaAdminOptions kafkaPtr RdKafkaAdminOpAny
  f (kafkaPtr, opts, queue)

withUnsafe :: [a]                                 -- ^ Items to handle
           -> (a -> IO (Either KafkaError b))     -- ^ Create an unsafe element
           -> ([b] -> IO ())                      -- ^ Destroy all unsafe elements
           -> ([b] -> IO c)                       -- ^ Handler
           -> IO (Either (NonEmpty KafkaError) c)
withUnsafe as mkOne cleanup f =
  bracket mkAll cleanupAll processAll
  where
    mkAll = partitionEithers <$> traverse mkOne as
    cleanupAll (_, ts') = cleanup ts'
    processAll (es, ts') =
      case es of
        []      -> Right <$> f ts'
        (e:es') -> pure $ Left (e :| es')

-- ^ Keeps applying a function until the error is found
--
-- Maybe it'be easier if internal helpers would return "ExceptT e m ()"?
whileRight :: Monad m
           => (a -> m (Either e ()))
           -> [a]
           -> m (Either e ())
whileRight f as = runExceptT $ traverse_ (ExceptT . f) as


-- ^ Polls the provided queue until it gets all the responses
-- from all the specified topics
waitForAllResponses :: [TopicName]
                    -> (RdKafkaEventTPtr -> IO (Maybe a))
                    -> (a -> IO [Either (Text, RdKafkaRespErrT, Text) Text])
                    -> RdKafkaQueueTPtr
                    -> IO [Either (TopicName, KafkaError, Text) TopicName]
waitForAllResponses ts fromEvent toResults q =
  fromMaybe [] <$> runMaybeT (go (S.fromList ts) [])
  where
    go awaited accRes = do
      qRes <- MaybeT $ rdKafkaQueuePoll q 1000
      eRes <- MaybeT $ fromEvent qRes
      tRes <- lift $ toResults eRes
      let results = wrapTopicName <$> tRes
      let topics  = S.fromList $ getTopicName <$> results
      let newRes = results <> accRes
      let remaining = S.difference awaited topics
      if S.null remaining
        then pure newRes
        else go remaining newRes

    getTopicName = either (\(t,_,_) -> t) id
    wrapTopicName = bimap (\(t,e,s) -> (TopicName t, KafkaResponseError e, s)) TopicName

