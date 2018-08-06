module Kafka.Admin
( module X
, ReplicationFactor(..)
, PartitionsCount(..)
, KafkaAdmin

, newKafkaAdmin
, createTopics
, closeKafkaAdmin
)
where

import           Control.Monad             (void)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Maybe
import           Data.Bifunctor
import qualified Data.Map                  as M
import           Data.Maybe                (fromMaybe)
import           Data.Set                  as S
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

createTopics :: KafkaAdmin
             -> [(TopicName, PartitionsCount, ReplicationFactor)]
             -> IO [Either (TopicName, KafkaError, String) TopicName]
createTopics client ts = do
  let topicNames = (\(t,_,_) -> t) <$> ts
  let kafkaPtr = acKafkaPtr client
  queue <- newRdKafkaQueue kafkaPtr
  topics <- newTopics
  case topics of
    Left err ->
      pure $ (\t -> Left (t, KafkaError err, err)) <$> topicNames
    Right topics' -> do
      rdKafkaCreateTopics kafkaPtr topics' (acOptions client) queue
      waitForAllResponses topicNames rdKafkaEventCreateTopicsResult rdKafkaCreateTopicsResultTopics queue
  where
    newTopics = sequence <$> traverse (\(t, p, r) -> mkNewTopic t p r) ts
    mkNewTopic (TopicName t) (PartitionsCount c) (ReplicationFactor r) = newRdKafkaNewTopic t c r

-------------------- Hepler servicing functions

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

