module Kafka.Producer.Callbacks
( deliveryCallback
, module X
)
where

import           Foreign.C.Error        (getErrno)
import           Foreign.Ptr            (Ptr, nullPtr)
import           Foreign.Storable       (Storable(peek))
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Types   (Offset(..))
import           Kafka.Internal.RdKafka (RdKafkaMessageT(..), RdKafkaRespErrT(..), rdKafkaConfSetDrMsgCb)
import           Kafka.Internal.Setup   (KafkaConf(..), getRdKafkaConf)
import           Kafka.Internal.Shared  (kafkaRespErr, readTopic, readKey, readPayload)
import           Kafka.Producer.Types   (DeliveryCallback(..), ProducerRecord(..), DeliveryReport(..), ProducePartition(..))
import           Kafka.Types            (KafkaError(..), TopicName(..))

-- | Sets the callback for delivery reports.
deliveryCallback :: (DeliveryReport -> IO ()) -> DeliveryCallback
deliveryCallback callback = DeliveryCallback callback
  --DeliveryCallback $ \kc -> rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  --where
  --  realCb :: t -> Ptr RdKafkaMessageT -> IO ()
  --  realCb _ mptr =
  --    if mptr == nullPtr
  --      then getErrno >>= (callback . NoMessageError . kafkaRespErr)
  --      else do
  --        s <- peek mptr
  --        if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
  --          then mkErrorReport s   >>= callback
  --          else mkSuccessReport s >>= callback

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
