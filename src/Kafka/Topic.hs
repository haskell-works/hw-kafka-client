module Kafka.Topic(
module X
, createTopic
, deleteTopic
) where

import           Control.Monad.IO.Class
import           Data.List.NonEmpty
import qualified Data.List.NonEmpty     as NEL
import           Data.Text
import qualified Data.Text              as T

import Kafka.Internal.RdKafka
import Kafka.Internal.Setup

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
      pure $ Right $ topicName topic

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
      pure $ Right topic

withNewTopic :: NewTopic
                -> (RdKafkaNewTopicTPtr ->  IO a)
                -> IO (Either (NonEmpty KafkaError) a)
withNewTopic t transform = do
  mkNewTopicRes <- mkNewTopic t newTopicPtr
  case mkNewTopicRes of
    Left err -> do
      return $ Left err
    Right topic -> do
      res <- transform topic
      return $ Right res

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

newTopicPtr :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
newTopicPtr topic = do
  ptrRes <- newRdKafkaNewTopic (unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)
  case ptrRes of
    Left str  -> pure $ Left (KafkaError $ T.pack str)
    Right ptr -> pure $ Right ptr

oldTopicPtr :: TopicName -> IO (Either KafkaError RdKafkaDeleteTopicTPtr)
oldTopicPtr tName = do
  res <- newRdKafkaDeleteTopic $ unpack . unTopicName $ tName
  case res of
    Left str  -> pure $ Left (KafkaError $ T.pack str)
    Right ptr -> pure $ Right ptr

mkNewTopic :: NewTopic
              -> (NewTopic -> IO (Either KafkaError a))
              -> IO (Either (NonEmpty KafkaError) a)
mkNewTopic topic create = do
  res <- create topic
  case res of
    Left err       -> pure $ Left (singletonList err)
    Right resource -> pure $ Right resource

rmOldTopic :: TopicName
            -> (TopicName -> IO (Either KafkaError a))
            -> IO (Either (NonEmpty KafkaError) a)
rmOldTopic tName remove = do
  res <- remove tName
  case res of
    Left err       -> pure $ Left (singletonList err)
    Right resource -> pure $ Right resource

singletonList :: a -> NonEmpty a
singletonList x = x :| []
