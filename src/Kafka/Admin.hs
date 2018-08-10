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
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Types

import Kafka.Admin.AdminProperties as X

newtype ReplicationFactor = ReplicationFactor { unReplicationFactor :: Int } deriving (Show, Eq, Read, Ord)
newtype PartitionsCount = PartitionsCount { unPartitionsCount :: Int } deriving (Show, Eq, Read, Ord)

data KafkaAdmin = KafkaAdmin
  { acKafkaPtr  :: !RdKafkaTPtr
  , acKafkaConf :: !KafkaConf
  , acOptions   :: !RdKafkaAdminOptionsTPtr
  }

data NewTopic = NewTopic
  { ntName              :: TopicName
  , ntPartitions        :: PartitionsCount
  , ntReplicationFactor :: ReplicationFactor
  , ntConfig            :: M.Map String String
  } deriving (Show)

newKafkaAdmin :: MonadIO m
               => AdminProperties
               -> m (Either KafkaError KafkaAdmin)
newKafkaAdmin props = liftIO $ do
  kc@(KafkaConf kc' _ _) <- kafkaConf (KafkaProps $ M.toList (apKafkaProps props)) --kafkaConf (KafkaProps [])
  mbKafka <- newRdKafkaT RdKafkaConsumer kc'
  case mbKafka of
    Left err    -> return . Left $ KafkaError err
    Right kafka -> do
      opts <- newRdKafkaAdminOptions kafka RdKafkaAdminOpAny
      return $ Right $ KafkaAdmin kafka kc opts

closeKafkaAdmin :: KafkaAdmin -> IO ()
closeKafkaAdmin client = void $ rdKafkaConsumerClose (acKafkaPtr client)

----------------------------- CREATE ------------------------------------------

createTopics :: KafkaAdmin
             -> [NewTopic]
             -> IO [Either (KafkaError, String) TopicName]
createTopics client ts = do
  let topicNames = ntName <$> ts
  let kafkaPtr = acKafkaPtr client
  queue <- newRdKafkaQueue kafkaPtr
  crRes <- withNewTopics ts $ \topics ->
             rdKafkaCreateTopics kafkaPtr topics (acOptions client) queue
  case crRes of
    Left es -> pure $ (\e -> Left (e, displayException e)) <$> NEL.toList es
    Right _ -> do
      res <- waitForAllResponses topicNames rdKafkaEventCreateTopicsResult rdKafkaCreateTopicsResultTopics queue
      pure $ first (\(_, a, b) -> (a, b)) <$> res

withNewTopics :: [NewTopic] -> ([RdKafkaNewTopicTPtr] ->  IO a) -> IO (Either (NonEmpty KafkaError) a)
withNewTopics ts =
  withUnsafe ts mkNewTopicUnsafe rdKafkaNewTopicDestroyArray

mkNewTopicUnsafe :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
mkNewTopicUnsafe NewTopic{..} = runExceptT $ do
  t <- withStrErr $ newRdKafkaNewTopicUnsafe (unTopicName ntName) (unPartitionsCount ntPartitions) (unReplicationFactor ntReplicationFactor)
  _ <- withKafkaErr $ whileRight (uncurry $ rdKafkaNewTopicSetConfig undefined) (M.toList ntConfig)
  pure t
  where
    withStrErr   = withExceptT KafkaError . ExceptT
    withKafkaErr = withExceptT KafkaResponseError . ExceptT


----------------------------- DELETE ------------------------------------------

deleteTopics :: KafkaAdmin
             -> [TopicName]
             -> IO [Either (TopicName, KafkaError, String) TopicName]
deleteTopics client ts = do
  let kafkaPtr = acKafkaPtr client
  queue <- newRdKafkaQueue kafkaPtr
  topics <- traverse (newRdKafkaDeleteTopic . unTopicName) ts
  rdKafkaDeleteTopics kafkaPtr topics (acOptions client) queue
  waitForAllResponses ts rdKafkaEventDeleteTopicsResult rdKafkaDeleteTopicsResultTopics queue

-------------------- Hepler servicing functions
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


-- ^ Polls the provided queue until it hets all the responses
-- from all the specified topics
waitForAllResponses :: [TopicName]
                    -> (RdKafkaEventTPtr -> IO (Maybe a))
                    -> (a -> IO [Either (String, RdKafkaRespErrT, String) String])
                    -> RdKafkaQueueTPtr
                    -> IO [Either (TopicName, KafkaError, String) TopicName]
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

