module Kafka.Dump
( hPrintSupportedKafkaConf
, hPrintKafka
, dumpKafkaConf
, dumpTopicConf
)
where

--
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup

import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Foreign
import           Foreign.C.String
import           System.IO

-- | Prints out all supported Kafka conf properties to a handle
hPrintSupportedKafkaConf :: MonadIO m => Handle -> m ()
hPrintSupportedKafkaConf h = liftIO $ handleToCFile h "w" >>= rdKafkaConfPropertiesShow

-- | Prints out all data associated with a specific kafka object to a handle
hPrintKafka :: (MonadIO m, HasKafka k) => Handle -> k -> m ()
hPrintKafka h k = liftIO $ handleToCFile h "w" >>= \f -> rdKafkaDump f (getRdKafka k)

-- | Returns a map of the current topic configuration
dumpTopicConf :: (MonadIO m, HasTopicConf t) => t -> m (Map String String)
dumpTopicConf t = liftIO $ parseDump (rdKafkaTopicConfDump (getRdTopicConf t))

-- | Returns a map of the current kafka configuration
dumpKafkaConf :: (MonadIO m, HasKafkaConf k) => k -> m (Map String String)
dumpKafkaConf k = liftIO $ parseDump (rdKafkaConfDump (getRdKafkaConf k))

parseDump :: (CSizePtr -> IO (Ptr CString)) -> IO (Map String String)
parseDump cstr = alloca $ \sizeptr -> do
    strPtr <- cstr sizeptr
    size <- peek sizeptr

    keysAndValues <- mapM (peekCString <=< peekElemOff strPtr) [0..(fromIntegral size - 1)]

    let ret = Map.fromList $ listToTuple keysAndValues
    rdKafkaConfDumpFree strPtr size
    return ret

listToTuple :: [String] -> [(String, String)]
listToTuple []      = []
listToTuple (k:v:t) = (k, v) : listToTuple t
listToTuple _       = error "list to tuple can only be called on even length lists"
