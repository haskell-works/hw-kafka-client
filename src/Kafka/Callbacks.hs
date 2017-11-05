module Kafka.Callbacks
( errorCallback
, logCallback
)
where

import Kafka.Internal.RdKafka
import Kafka.Internal.Setup
import Kafka.Types

errorCallback :: HasKafkaConf k => (KafkaError -> String -> IO ()) -> k -> IO ()
errorCallback callback k =
  let (KafkaConf c _) = getKafkaConf k
      realCb _ err = callback (KafkaResponseError err)
  in rdKafkaConfSetErrorCb c realCb

logCallback :: HasKafkaConf k => (Int -> String -> String -> IO ()) -> k -> IO ()
logCallback callback k =
  let (KafkaConf c _) = getKafkaConf k
      realCb _ = callback
  in rdKafkaConfSetLogCb c realCb
