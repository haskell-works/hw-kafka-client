module Kafka.Internal.Shared
where

import           Control.Exception
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Internal   as BSI
import           Foreign.C.Error
import           Kafka
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum

word8PtrToBS :: Int -> Word8Ptr -> IO BS.ByteString
word8PtrToBS len ptr = BSI.create len $ \bsptr ->
    BSI.memcpy bsptr ptr len

kafkaRespErr :: Errno -> KafkaError
kafkaRespErr (Errno num) = KafkaResponseError $ rdKafkaErrno2err (fromIntegral num)
{-# INLINE kafkaRespErr #-}

throwOnError :: IO (Maybe String) -> IO ()
throwOnError action = do
    m <- action
    case m of
        Just e -> throw $ KafkaError e
        Nothing -> return ()

hasError :: KafkaError -> Bool
hasError err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> False
    _ -> True
{-# INLINE hasError #-}

kafkaErrorToEither :: KafkaError -> Either KafkaError ()
kafkaErrorToEither err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> Right ()
    _ -> Left err
{-# INLINE kafkaErrorToEither #-}

kafkaErrorToMaybe :: KafkaError -> Maybe KafkaError
kafkaErrorToMaybe err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> Nothing
    _ -> Just err
{-# INLINE kafkaErrorToMaybe #-}
