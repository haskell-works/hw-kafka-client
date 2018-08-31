module Kafka.Callbacks
( errorCallback
, logCallback
, statsCallback
)
where

import Kafka.Internal.RdKafka (rdKafkaConfSetErrorCb, rdKafkaConfSetLogCb, rdKafkaConfSetStatsCb)
import Kafka.Internal.Setup (HasKafkaConf(..), getRdKafkaConf)
import Kafka.Types (KafkaError(..))

errorCallback :: HasKafkaConf k => (KafkaError -> String -> IO ()) -> k -> IO ()
errorCallback callback k =
  let realCb _ err = callback (KafkaResponseError err)
  in rdKafkaConfSetErrorCb (getRdKafkaConf k) realCb

logCallback :: HasKafkaConf k => (Int -> String -> String -> IO ()) -> k -> IO ()
logCallback callback k =
  let realCb _ = callback
  in rdKafkaConfSetLogCb (getRdKafkaConf k) realCb

statsCallback :: HasKafkaConf k => (String -> IO ()) -> k -> IO ()
statsCallback callback k =
  let realCb _ = callback
  in rdKafkaConfSetStatsCb (getRdKafkaConf k) realCb
