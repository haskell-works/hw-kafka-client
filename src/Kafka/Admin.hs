module Kafka.Admin(
module X
, newKAdmin
, createTopic
, closeKAdmin
) where

import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import Control.Monad.IO.Class
import Data.Text
import Data.Maybe
import Data.Bifunctor
import Data.List.NonEmpty
import qualified Data.List.NonEmpty as NEL
import qualified Data.Text as T
import qualified Data.Set as S

import Kafka.Internal.RdKafka
import Kafka.Internal.Setup

import Kafka.Types as X
import Kafka.Admin.AdminProperties as X
import Kafka.Admin.Types as X

newKAdmin ::( MonadIO m )
          => AdminProperties      
          -> m (Either KafkaError KAdmin)
newKAdmin properties = liftIO $ do
  kafkaConfig@(KafkaConf kafkaConf' _ _) <- kafkaConf ( KafkaProps $ adminProps properties)
  maybeKafka <- newRdKafkaT RdKafkaConsumer kafkaConf'
  case maybeKafka of
    Left err -> pure $ Left $ KafkaError err
    Right kafka -> pure $ Right $ KAdmin (Kafka kafka) kafkaConfig

closeKAdmin :: KAdmin 
               -> IO ()
closeKAdmin ka = void $ rdKafkaConsumerClose (getRdKafka ka)
--- CREATE TOPIC ---
createTopic :: KAdmin
              -> NewTopic
              -> IO (Either KafkaError TopicName)
createTopic kAdmin topic = liftIO $ do
  let kafkaPtr = getRdKafka kAdmin
  queue <- newRdKafkaQueue kafkaPtr
  opts <- newRdKAdminOptions kafkaPtr RdKafkaAdminOpAny 

  topicRes <- withNewTopic topic $ \topic' -> rdKafkaCreateTopic kafkaPtr topic' opts queue
  case topicRes of
    Left err -> do 
      pure $ Left (NEL.head err)
    Right _ -> do
      pure $ Right $ topicName topic

withNewTopic :: NewTopic 
                -> (RdKafkaNewTopicTPtr ->  IO a)
                -> IO (Either (NonEmpty KafkaError) a)
withNewTopic t transform = do
  mkNewTopicRes <- mkNewTopic t topicPtr
  case mkNewTopicRes of
    Left err -> do
      return $ Left err
    Right topic -> do 
      res <- transform topic
      return $ Right res

topicPtr :: NewTopic -> IO (Either KafkaError RdKafkaNewTopicTPtr)
topicPtr topic = do
  ptrRes <- newRdKafkaNewTopic (unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)
  case ptrRes of
    Left str -> pure $ Left (KafkaError $ T.pack str)
    Right ptr -> pure $ Right ptr

mkNewTopic :: NewTopic 
              -> (NewTopic -> IO (Either KafkaError a))
              -> IO (Either (NonEmpty KafkaError) a)
mkNewTopic topic create = do
  res <- create topic
  case res of
    Left err -> pure $ Left (Data.List.NonEmpty.singleton err)
    Right resource -> pure $ Right resource

