module Kafka.Internal.Dump
where

--
import           Kafka.Internal.RdKafka

import           Control.Monad
import           Data.Map.Strict            (Map)
import           Foreign
import           Foreign.C.String
import           System.IO
import qualified Data.Map.Strict            as Map

-- | Prints out all supported Kafka conf properties to a handle
hPrintSupportedKafkaConf :: Handle -> IO ()
hPrintSupportedKafkaConf h = handleToCFile h "w" >>= rdKafkaConfPropertiesShow

-- | Prints out all data associated with a specific kafka object to a handle
hPrintKafka :: Handle -> RdKafkaTPtr -> IO ()
hPrintKafka h k = handleToCFile h "w" >>= \f -> rdKafkaDump f k

-- | Returns a map of the current kafka configuration
dumpConfFromKafka :: RdKafkaConfTPtr -> IO (Map String String)
dumpConfFromKafka = dumpKafkaConf

-- | Returns a map of the current topic configuration
dumpConfFromKafkaTopic :: RdKafkaTopicConfTPtr -> IO (Map String String)
dumpConfFromKafkaTopic = dumpTopicConf

dumpTopicConf :: RdKafkaTopicConfTPtr -> IO (Map String String)
dumpTopicConf kptr = parseDump (rdKafkaTopicConfDump kptr)

dumpKafkaConf :: RdKafkaConfTPtr -> IO (Map String String)
dumpKafkaConf kptr = parseDump (rdKafkaConfDump kptr)

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
