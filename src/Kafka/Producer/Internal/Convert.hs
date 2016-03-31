module Kafka.Producer.Internal.Convert
where

import           Control.Monad
import           Foreign.C.Error
import           Foreign.C.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Shared
import           Kafka.Internal.Types
import           Kafka.Producer.Internal.Types

copyMsgFlags :: Int
copyMsgFlags = rdKafkaMsgFlagCopy
{-# INLINE copyMsgFlags  #-}

producePartitionInteger :: KafkaProducePartition -> CInt
producePartitionInteger KafkaUnassignedPartition = -1
producePartitionInteger (KafkaSpecifiedPartition n) = fromIntegral n
{-# INLINE producePartitionInteger #-}

handleProduceErr :: Int -> IO (Maybe KafkaError)
handleProduceErr (- 1) = liftM (Just . kafkaRespErr) getErrno
handleProduceErr 0 = return Nothing
handleProduceErr _ = return $ Just KafkaInvalidReturnValue
{-# INLINE handleProduceErr #-}
