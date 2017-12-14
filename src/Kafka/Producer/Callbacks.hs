module Kafka.Producer.Callbacks
( deliveryErrorsCallback
, module X
)
where

import Foreign
import Foreign.C.Error
import Kafka.Callbacks        as X
import Kafka.Internal.RdKafka
import Kafka.Internal.Setup
import Kafka.Internal.Shared
import Kafka.Types

-- | Sets the callback for delivery errors.
-- The callback is only called in case of errors.
deliveryErrorsCallback :: (KafkaError -> IO ()) -> KafkaConf -> IO ()
deliveryErrorsCallback callback kc = rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr =
      if mptr == nullPtr
        then getErrno >>= (callback . kafkaRespErr)
        else do
          s <- peek mptr
          if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then (callback . KafkaResponseError $ err'RdKafkaMessageT s)
            else pure ()
