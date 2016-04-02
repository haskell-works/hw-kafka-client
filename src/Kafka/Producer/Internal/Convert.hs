module Kafka.Producer.Internal.Convert
where

import           Control.Monad
import           Foreign.C.Error
import           Foreign.C.Types
import           Kafka
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Shared
import           Kafka.Producer.Internal.Types

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
handleProduceErr (- 1) = liftM (Just . kafkaRespErr) getErrno
handleProduceErr 0 = return Nothing
handleProduceErr _ = return $ Just KafkaInvalidReturnValue
{-# INLINE handleProduceErr #-}
