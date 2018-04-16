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

-- | Sets the callback for delivery errors.
-- The callback is only called in case of errors.
deliveryCallback :: (Either ProducerError ProducerSuccess -> IO ()) -> KafkaConf -> IO ()
deliveryCallback callback kc = rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr =
      if mptr == nullPtr
        then getErrno >>= (callback . Left . errNoMessage . kafkaRespErr)
        else do
          s <- peek mptr
          if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then do
              r <- mkErrorReport s
              callback (Left r)
            else do
              r <- mkSuccessReport s
              callback (Right r)
    errNoMessage ke = ProducerError { peValue = Nothing, peError = ke }

mkErrorReport :: RdKafkaMessageT -> IO ProducerError
mkErrorReport p = do
  payload <- readPayload p
  pure ProducerError
    { peError = KafkaResponseError (err'RdKafkaMessageT p)
    , peValue = payload
    }

mkSuccessReport :: RdKafkaMessageT -> IO ProducerSuccess
mkSuccessReport msg = do
  topic     <- readTopic msg
  key       <- readKey msg
  payload   <- readPayload msg
  pure ProducerSuccess
    { psTopic     = TopicName topic
    , psPartition = PartitionId $ partition'RdKafkaMessageT msg
    , psOffset    = Offset $ offset'RdKafkaMessageT msg
    , psKey       = key
    , psValue     = payload
    }
