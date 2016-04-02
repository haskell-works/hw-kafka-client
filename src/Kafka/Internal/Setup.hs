module Kafka.Internal.Setup where

import           Kafka
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum

import           Control.Exception
import           Control.Monad
import           Data.Map.Strict            (Map)
import           Foreign
import           Foreign.C.String
import           System.IO

import qualified Data.Map.Strict            as Map

-- | Adds a broker string to a given kafka instance.
addBrokers :: Kafka -> String -> IO ()
addBrokers (Kafka kptr _) brokerStr = do
    numBrokers <- rdKafkaBrokersAdd kptr brokerStr
    when (numBrokers == 0)
        (throw $ KafkaBadSpecification "No valid brokers specified")

-- | Create kafka object with the given configuration. Most of the time
-- you will not need to use this function directly
newKafka :: RdKafkaTypeT -> KafkaProps -> IO Kafka
newKafka kafkaType props = kafkaConf props >>= newKafkaPtr kafkaType

-- | Create a kafka topic object with the given configuration. Most of the
-- time you will not need to use this function directly
newKafkaTopic :: Kafka -> String -> TopicProps -> IO KafkaTopic
newKafkaTopic k tName overrides = topicConf overrides >>= newKafkaTopicPtr k tName

newKafkaPtr :: RdKafkaTypeT -> KafkaConf -> IO Kafka
newKafkaPtr kafkaType c@(KafkaConf confPtr) = do
    et <- newRdKafkaT kafkaType confPtr
    case et of
        Left e -> error e
        Right x -> return $ Kafka x c

newKafkaTopicPtr :: Kafka -> String -> TopicConf -> IO KafkaTopic
newKafkaTopicPtr k@(Kafka kPtr _) tName conf@(TopicConf confPtr) = do
    et <- newRdKafkaTopicT kPtr tName confPtr
    case et of
        Left e -> throw $ KafkaError e
        Right x -> return $ KafkaTopic x k conf

--
-- Configuration
--

newTopicConf :: IO TopicConf
newTopicConf = liftM TopicConf newRdKafkaTopicConfT

newKafkaConf :: IO KafkaConf
newKafkaConf = liftM KafkaConf newRdKafkaConfT

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
setKafkaConfValue (KafkaConf confPtr) key value =
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


--
-- Dumping
--
-- | Prints out all supported Kafka conf properties to a handle
hPrintSupportedKafkaConf :: Handle -> IO ()
hPrintSupportedKafkaConf h = handleToCFile h "w" >>= rdKafkaConfPropertiesShow

-- | Prints out all data associated with a specific kafka object to a handle
hPrintKafka :: Handle -> Kafka -> IO ()
hPrintKafka h k = handleToCFile h "w" >>= \f -> rdKafkaDump f (kafkaPtr k)

-- | Returns a map of the current kafka configuration
dumpConfFromKafka :: Kafka -> IO (Map String String)
dumpConfFromKafka (Kafka _ cfg) = dumpKafkaConf cfg

-- | Returns a map of the current topic configuration
dumpConfFromKafkaTopic :: KafkaTopic -> IO (Map String String)
dumpConfFromKafkaTopic (KafkaTopic _ _ conf) = dumpTopicConf conf

dumpTopicConf :: TopicConf -> IO (Map String String)
dumpTopicConf (TopicConf kptr) = parseDump (rdKafkaTopicConfDump kptr)

dumpKafkaConf :: KafkaConf -> IO (Map String String)
dumpKafkaConf (KafkaConf kptr) = parseDump (rdKafkaConfDump kptr)

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map String String)
parseDump cstr = alloca $ \sizeptr -> do
    strPtr <- cstr sizeptr
    size <- peek sizeptr

    keysAndValues <- mapM (peekCString <=< peekElemOff strPtr) [0..(fromIntegral size - 1)]

    let ret = Map.fromList $ listToTuple keysAndValues
    rdKafkaConfDumpFree strPtr size
    return ret

listToTuple :: [String] -> [(String, String)]
listToTuple [] = []
listToTuple (k:v:t) = (k, v) : listToTuple t
listToTuple _ = error "list to tuple can only be called on even length lists"
