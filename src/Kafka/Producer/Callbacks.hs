module Kafka.Producer.Callbacks
( deliveryCallback
, module X
)
where

import           Foreign
import           Foreign.C.Error
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Internal.Shared
import           Kafka.Producer.Types
import           Kafka.Types

-- | Sets the callback for delivery reports.
deliveryCallback :: (DeliveryReport -> IO ()) -> KafkaConf -> IO ()
deliveryCallback callback kc = rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr =
      if mptr == nullPtr
        then getErrno >>= (callback . NoMessageError . kafkaRespErr)
        else do
          s <- peek mptr
          if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then mkErrorReport s   >>= callback
            else mkSuccessReport s >>= callback

mkErrorReport :: RdKafkaMessageT -> IO DeliveryReport
mkErrorReport msg = do
  prodRec <- mkProdRec msg
  pure $ DeliveryFailure prodRec (KafkaResponseError (err'RdKafkaMessageT msg))

mkSuccessReport :: RdKafkaMessageT -> IO DeliveryReport
mkSuccessReport msg = do
  prodRec <- mkProdRec msg
  pure $ DeliverySuccess prodRec (Offset $ offset'RdKafkaMessageT msg)

mkProdRec :: RdKafkaMessageT -> IO ProducerRecord
mkProdRec msg = do
  topic     <- readTopic msg
  key       <- readKey msg
  payload   <- readPayload msg
  pure ProducerRecord
    { prTopic = TopicName topic
    , prPartition = SpecifiedPartition (partition'RdKafkaMessageT msg)
    , prKey = key
    , prValue = payload
    }
