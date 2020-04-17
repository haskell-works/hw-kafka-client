module Kafka.Callbacks
( errorCallback
, logCallback
, statsCallback
)
where

import Kafka.Internal.RdKafka (rdKafkaConfSetErrorCb, rdKafkaConfSetLogCb, rdKafkaConfSetStatsCb)
import Kafka.Internal.Setup (HasKafkaConf(..), getRdKafkaConf)
import Kafka.Types (KafkaError(..), KafkaLogLevel(..))

-- | Add a callback for errors.
--
-- ==== __Examples__
--
-- Basic usage:
--
-- > setCallback (errorCallback myErrorCallback)
-- >
-- > myErrorCallback :: KafkaError -> String -> IO ()
-- > myErrorCallback kafkaError message = print $ show kafkaError <> "|" <> message
errorCallback :: HasKafkaConf k => (KafkaError -> String -> IO ()) -> k -> IO ()
errorCallback callback k =
  let realCb _ err = callback (KafkaResponseError err)
  in rdKafkaConfSetErrorCb (getRdKafkaConf k) realCb

-- | Add a callback for logs.
--
-- ==== __Examples__
--
-- Basic usage:
--
-- > setCallback (logCallback myLogCallback)
-- >
-- > myLogCallback :: KafkaLogLevel -> String -> String -> IO ()
-- > myLogCallback level facility message = print $ show level <> "|" <> facility <> "|" <> message
logCallback :: HasKafkaConf k => (KafkaLogLevel -> String -> String -> IO ()) -> k -> IO ()
logCallback callback k =
  let realCb _ = callback . toEnum
  in rdKafkaConfSetLogCb (getRdKafkaConf k) realCb

-- | Add a callback for stats.
--
-- ==== __Examples__
--
-- Basic usage:
--
-- > setCallback (statsCallback myStatsCallback)
-- >
-- > myStatsCallback :: String -> IO ()
-- > myStatsCallback stats = print $ show stats
statsCallback :: HasKafkaConf k => (String -> IO ()) -> k -> IO ()
statsCallback callback k =
  let realCb _ = callback
  in rdKafkaConfSetStatsCb (getRdKafkaConf k) realCb
