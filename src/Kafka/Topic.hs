module Kafka.Topic(
module X
, createTopic
, deleteTopic
) where

import           Control.Exception
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Except
import           Control.Monad.Trans.Maybe
import           Data.Bifunctor
import           Data.Foldable
import           Data.List.NonEmpty
import qualified Data.List.NonEmpty         as NEL
import qualified Data.Map                   as M
import           Data.Maybe
import qualified Data.Set                   as S
import qualified Data.Text                  as T
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup

import Kafka.Topic.Types as X
import Kafka.Types       as X

--- CREATE TOPIC ---
createTopic :: HasKafka k => k -> NewTopic -> IO (Either KafkaError TopicName)
createTopic k topic = do
  let kafkaPtr = getRdKafka k
  queue <- newRdKafkaQueue kafkaPtr
  opts <- newRdKAdminOptions kafkaPtr RdKafkaAdminOpAny

  topicRes <- withNewTopic topic $ \topic' -> rdKafkaCreateTopic kafkaPtr topic' opts queue

  case topicRes of
    Left err -> do
      pure $ Left (NEL.head err)
    Right _ -> do
      res <- waitForResponse (topicName topic) rdKafkaEventCreateTopicsResult rdKafkaCreateTopicsResultTopics queue
      case listToMaybe res of
        Nothing -> pure $ Left KafkaInvalidReturnValue
        Just result -> pure $ case result of
          Left (_, e, _) -> Left e
          Right tName    -> Right tName

--- DELETE TOPIC ---
deleteTopic :: HasKafka k
              => k
              -> TopicName
              -> IO (Either KafkaError TopicName)
deleteTopic k topic = liftIO $ do
  let kafkaPtr = getRdKafka k
  queue <- newRdKafkaQueue kafkaPtr
  opts <- newRdKAdminOptions kafkaPtr RdKafkaAdminOpAny

  topicRes <- withOldTopic topic $ \topic' -> rdKafkaDeleteTopics kafkaPtr [topic'] opts queue
  case topicRes of
    Left err -> do
      pure $ Left (NEL.head err)
    Right _ -> do
      res <- waitForResponse topic rdKafkaEventDeleteTopicsResult rdKafkaDeleteTopicsResultTopics queue
      case listToMaybe res of
        Nothing -> pure $ Left KafkaInvalidReturnValue
        Just result -> pure $ case result of
          Left (_, e, _) -> Left e
          Right tName    -> Right tName

withNewTopic :: NewTopic
                -> (RdKafkaNewTopicTPtr ->  IO a)
                -> IO (Either (NonEmpty KafkaError) a)
withNewTopic t = withUnsafeOne t mkNewTopicUnsafe rdKafkaNewTopicDestroy

withOldTopic :: TopicName
                -> (RdKafkaDeleteTopicTPtr -> IO a)
                -> IO (Either (NonEmpty KafkaError) a)
withOldTopic tName transform = do
  rmOldTopicRes <- rmOldTopic tName oldTopicPtr
  case rmOldTopicRes of
    Left err -> do
      return $ Left err
    Right topic -> do
      res <- transform topic
      return $ Right res

oldTopicPtr :: TopicName -> IO (Either KafkaError RdKafkaDeleteTopicTPtr)
oldTopicPtr tName = do
  res <- newRdKafkaDeleteTopic $ T.unpack . unTopicName $ tName
  case res of
    Left str  -> pure $ Left (KafkaError $ T.pack str)
    Right ptr -> pure $ Right ptr

mkNewTopicUnsafe :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
mkNewTopicUnsafe topic = runExceptT $ do
  topic' <- withErrStr $ newRdKafkaNewTopicUnsafe (T.unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)
  _ <- withErrKafka $ whileRight (uncurry $ rdKafkaNewTopicSetConfig undefined) (M.toList $ topicConfig topic)
  pure topic'
    where
      withErrStr = withExceptT (KafkaError . T.pack) . ExceptT
      withErrKafka = withExceptT KafkaResponseError . ExceptT

rmOldTopic :: TopicName
            -> (TopicName -> IO (Either KafkaError a))
            -> IO (Either (NonEmpty KafkaError) a)
rmOldTopic tName remove = do
  res <- remove tName
  case res of
    Left err       -> pure $ Left (singletonList err)
    Right resource -> pure $ Right resource

withUnsafeOne :: a                               -- ^ Item to handle
              -> (a -> IO (Either KafkaError b)) -- ^ Create an unsafe element
              -> (b -> IO ())                    -- ^ Destroy the unsafe element
              -> (b -> IO c)                     -- ^ Handler
              -> IO (Either (NonEmpty KafkaError) c)
withUnsafeOne a mkOne cleanup f =
  bracket (mkOne a) cleanupOne processOne
  where
    cleanupOne (Right b) = cleanup b
    cleanupOne (Left _)  = pure () -- no resource to clean if creation failed

    processOne (Right b) = Right <$> f b
    processOne (Left e)  = pure (Left (singletonList e))

whileRight :: Monad m
           => (a -> m (Either e ()))
           -> [a]
           -> m (Either e ())
whileRight f as = runExceptT $ traverse_ (ExceptT . f) as

waitForResponse :: TopicName
                -> (RdKafkaEventTPtr -> IO (Maybe a))
                -> (a -> IO [Either (String, RdKafkaRespErrT, String) String])
                -> RdKafkaQueueTPtr
                -> IO [Either (TopicName, KafkaError, String) TopicName]
waitForResponse topic fromEvent toResults q =
  fromMaybe [] <$> runMaybeT (go [])
  where
    awaited = S.singleton topic

    go accRes = do
      qRes <- MaybeT $ rdKafkaQueuePoll q 1000
      eRes <- MaybeT $ fromEvent qRes
      tRes <- lift $ toResults eRes
      let results = wrapTopicName <$> tRes
      let topics  = S.fromList $ getTopicName <$> results
      let newRes  = results <> accRes
      let remaining = S.difference awaited topics
      if S.null remaining
        then pure newRes
        else go newRes

    getTopicName = either (\(t,_,_) -> t) id
    wrapTopicName = bimap (\(t,e,s) -> (TopicName (T.pack t), KafkaResponseError e, s))
                          (TopicName . T.pack)

singletonList :: a -> NonEmpty a
singletonList x = x :| []

