module Kafka.Internal.Setup
( KafkaProps(..)
, TopicProps(..)
, Kafka(..)
, KafkaConf(..)
, TopicConf(..)
, HasKafka(..)
, HasKafkaConf(..)
, HasTopicConf(..)
, getRdKafka
, getRdKafkaConf
, getRdMsgQueue
, getRdTopicConf
, newTopicConf
, newKafkaConf
, kafkaConf
, topicConf
, checkConfSetValue
, setKafkaConfValue
, setAllKafkaConfValues
, setTopicConfValue
, setAllTopicConfValues
)
where

import Kafka.Internal.RdKafka (RdKafkaConfResT(..), CCharBufPointer, RdKafkaQueueTPtr, RdKafkaTPtr, RdKafkaConfTPtr, RdKafkaTopicConfTPtr, nErrorBytes, rdKafkaTopicConfSet, newRdKafkaTopicConfT, newRdKafkaConfT, rdKafkaConfSet)
import Kafka.Types (KafkaError(..))

import Control.Exception (throw)
import Data.IORef (IORef, newIORef, readIORef)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.C.String (peekCString)
import Kafka.Internal.CancellationToken (CancellationToken(..), newCancellationToken)
import qualified Data.Text as Text
import Data.Map (Map)
import Data.Text (Text)
import qualified Data.Map as Map

--
-- Configuration
--
newtype KafkaProps = KafkaProps (Map Text Text) deriving (Show, Eq)
newtype TopicProps = TopicProps (Map Text Text) deriving (Show, Eq)
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
      throw $ KafkaInvalidConfigurationValue (Text.pack str)
    RdKafkaConfUnknown -> do
      str <- peekCString charPtr
      throw $ KafkaUnknownConfigurationKey (Text.pack str)

setKafkaConfValue :: KafkaConf -> Text -> Text -> IO ()
setKafkaConfValue (KafkaConf confPtr _ _) key value =
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaConfSet confPtr (Text.unpack key) (Text.unpack value) charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllKafkaConfValues :: KafkaConf -> KafkaProps -> IO ()
setAllKafkaConfValues conf (KafkaProps props) = Map.foldMapWithKey (setKafkaConfValue conf) props --forM_ props $ uncurry (setKafkaConfValue conf)

setTopicConfValue :: TopicConf -> Text -> Text -> IO ()
setTopicConfValue (TopicConf confPtr) key value =
  allocaBytes nErrorBytes $ \charPtr -> do
    err <- rdKafkaTopicConfSet confPtr (Text.unpack key) (Text.unpack value) charPtr (fromIntegral nErrorBytes)
    checkConfSetValue err charPtr

setAllTopicConfValues :: TopicConf -> TopicProps -> IO ()
setAllTopicConfValues conf (TopicProps props) = Map.foldMapWithKey (setTopicConfValue conf) props --forM_ props $ uncurry (setTopicConfValue conf)
