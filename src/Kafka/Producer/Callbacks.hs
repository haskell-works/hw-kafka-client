{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE LambdaCase #-}
module Kafka.Producer.Callbacks
( deliveryCallback
, module X
)
where

import           Control.Monad          (void)
import           Control.Exception      (bracket)
import           Control.Concurrent     (forkIO)
import           Foreign.C.Error        (getErrno)
import           Foreign.Ptr            (Ptr, nullPtr)
import           Foreign.Storable       (Storable(peek))
import           Foreign.StablePtr      (castPtrToStablePtr, deRefStablePtr, freeStablePtr)
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Types   (Offset(..))
import           Kafka.Internal.RdKafka (RdKafkaMessageT(..), RdKafkaRespErrT(..), rdKafkaConfSetDrMsgCb)
import           Kafka.Internal.Setup   (getRdKafkaConf, Callback(..))
import           Kafka.Internal.Shared  (kafkaRespErr, readTopic, readKey, readPayload, readHeaders)
import           Kafka.Producer.Types   (ProducerRecord(..), DeliveryReport(..), ProducePartition(..))
import           Kafka.Types            (KafkaError(..), TopicName(..))
import Data.Either (fromRight)

-- | Sets the callback for delivery reports.
--
--   /Note: A callback should not be a long-running process as it blocks
--   librdkafka from continuing on the thread that handles the delivery
--   callbacks. For callbacks to individual messsages see
--   'Kafka.Producer.produceMessage\''./
--
deliveryCallback :: (DeliveryReport -> IO ()) -> Callback
deliveryCallback callback = Callback $ \kc -> rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr =
      if mptr == nullPtr
        then getErrno >>= (callback . NoMessageError . kafkaRespErr)
        else do
          s <- peek mptr
          prodRec <- mkProdRec mptr
          let cbPtr = opaque'RdKafkaMessageT s
          callbacks cbPtr $ 
            if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
              then mkErrorReport s prodRec  
              else mkSuccessReport s prodRec

    callbacks cbPtr rep = do
      callback rep
      if cbPtr == nullPtr then
        pure ()
      else bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr $ \stablePtr -> do
        msgCb <- deRefStablePtr @(DeliveryReport -> IO ()) stablePtr
        -- Here we fork the callback since it might be a longer action and
        -- blocking here would block librdkafka from continuing its execution
        void . forkIO $ msgCb rep

mkErrorReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkErrorReport msg prodRec = DeliveryFailure prodRec (KafkaResponseError (err'RdKafkaMessageT msg))

mkSuccessReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkSuccessReport msg prodRec = DeliverySuccess prodRec (Offset $ offset'RdKafkaMessageT msg)

mkProdRec :: Ptr RdKafkaMessageT -> IO ProducerRecord
mkProdRec pmsg = do
  msg         <- peek pmsg  
  topic       <- readTopic msg
  key         <- readKey msg
  payload     <- readPayload msg
  flip fmap (fromRight mempty <$> readHeaders pmsg) $ \headers -> 
    ProducerRecord
      { prTopic = TopicName topic
      , prPartition = SpecifiedPartition (partition'RdKafkaMessageT msg)
      , prKey = key
      , prValue = payload
      , prHeaders = headers
      }
