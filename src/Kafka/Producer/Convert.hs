module Kafka.Producer.Convert
( copyMsgFlags
, producePartitionInt
, producePartitionCInt
, handleProduceErr
, handleProduceErr'
)
where

import           Foreign.C.Error        (getErrno)
import           Foreign.C.Types        (CInt)
import           Kafka.Internal.RdKafka (rdKafkaMsgFlagCopy)
import           Kafka.Internal.Shared  (kafkaRespErr)
import           Kafka.Types            (KafkaError(..))
import           Kafka.Producer.Types   (ProducePartition(..))

copyMsgFlags :: Int
copyMsgFlags = rdKafkaMsgFlagCopy
{-# INLINE copyMsgFlags  #-}

producePartitionInt :: ProducePartition -> Int
producePartitionInt UnassignedPartition = -1
producePartitionInt (SpecifiedPartition n) = n
{-# INLINE producePartitionInt #-}

producePartitionCInt :: ProducePartition -> CInt
producePartitionCInt = fromIntegral . producePartitionInt
{-# INLINE producePartitionCInt #-}

handleProduceErr :: Int -> IO (Maybe KafkaError)
handleProduceErr (- 1) = Just . kafkaRespErr <$> getErrno
handleProduceErr 0 = return Nothing
handleProduceErr _ = return $ Just KafkaInvalidReturnValue
{-# INLINE handleProduceErr #-}

handleProduceErr' :: Int -> IO (Either KafkaError ())
handleProduceErr' (- 1) = Left . kafkaRespErr <$> getErrno
handleProduceErr' 0 = return (Right ())
handleProduceErr' _ = return $ Left KafkaInvalidReturnValue
{-# INLINE handleProduceErr' #-}
