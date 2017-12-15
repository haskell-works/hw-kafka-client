module Kafka.Internal.Setup where

import Kafka.Internal.RdKafka
import Kafka.Types

import Control.Exception
import Control.Monad
import Data.IORef
import Foreign
import Foreign.C.String
import Kafka.Internal.CancellationToken

--
-- Configuration
--
newtype KafkaProps = KafkaProps [(String, String)] deriving (Show, Eq)
newtype TopicProps = TopicProps [(String, String)] deriving (Show, Eq)
newtype Kafka      = Kafka RdKafkaTPtr deriving Show
data KafkaConf     = KafkaConf RdKafkaConfTPtr (IORef (Maybe RdKafkaQueueTPtr)) CancellationToken
newtype TopicConf  = TopicConf RdKafkaTopicConfTPtr deriving Show

class HasKafka a where
  getKafka :: a -> Kafka

class HasKafkaConf a where
  getKafkaConf :: a -> KafkaConf

class HasTopicConf a where
  getTopicConf :: a -> TopicConf

instance HasKafkaConf KafkaConf where
  getKafkaConf = id
  {-# INLINE getKafkaConf #-}

instance HasKafka Kafka where
  getKafka = id
  {-# INLINE getKafka #-}

instance HasTopicConf TopicConf where
  getTopicConf = id
  {-# INLINE getTopicConf #-}

getRdKafka :: HasKafka k => k -> RdKafkaTPtr
getRdKafka k = let (Kafka k') = getKafka k in k'
{-# INLINE getRdKafka #-}

getRdKafkaConf :: HasKafkaConf k => k -> RdKafkaConfTPtr
getRdKafkaConf k = let (KafkaConf k' _ _) = getKafkaConf k in k'
{-# INLINE getRdKafkaConf #-}

getRdMsgQueue :: HasKafkaConf k => k -> IO (Maybe RdKafkaQueueTPtr)
getRdMsgQueue k =
  let (KafkaConf _ rq _) = getKafkaConf k
  in readIORef rq

getRdTopicConf :: HasTopicConf t => t -> RdKafkaTopicConfTPtr
getRdTopicConf t = let (TopicConf t') = getTopicConf t in t'
{-# INLINE getRdTopicConf #-}

newTopicConf :: IO TopicConf
newTopicConf = TopicConf <$> newRdKafkaTopicConfT

newKafkaConf :: IO KafkaConf
newKafkaConf = KafkaConf <$> newRdKafkaConfT <*> newIORef Nothing <*> newCancellationToken

kafkaConf :: KafkaProps -> IO KafkaConf
kafkaConf overrides = do
  conf <- newKafkaConf
  setAllKafkaConfValues conf overrides
  return conf

topicConf :: TopicProps -> IO TopicConf
topicConf overrides = do
  conf <- newTopicConf
  setAllTopicConfValues conf overrides
  return conf

checkConfSetValue :: RdKafkaConfResT -> CCharBufPointer -> IO ()
checkConfSetValue err charPtr = case err of
    RdKafkaConfOk -> return ()
    RdKafkaConfInvalid -> do
      str <- peekCString charPtr
      throw $ KafkaInvalidConfigurationValue str
    RdKafkaConfUnknown -> do
      str <- peekCString charPtr
      throw $ KafkaUnknownConfigurationKey str

setKafkaConfValue :: KafkaConf -> String -> String -> IO ()
setKafkaConfValue (KafkaConf confPtr _ _) key value =
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaConfSet confPtr key value charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllKafkaConfValues :: KafkaConf -> KafkaProps -> IO ()
setAllKafkaConfValues conf (KafkaProps props) = forM_ props $ uncurry (setKafkaConfValue conf)

setTopicConfValue :: TopicConf -> String -> String -> IO ()
setTopicConfValue (TopicConf confPtr) key value =
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaTopicConfSet confPtr key value charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllTopicConfValues :: TopicConf -> TopicProps -> IO ()
setAllTopicConfValues conf (TopicProps props) = forM_ props $ uncurry (setTopicConfValue conf)
