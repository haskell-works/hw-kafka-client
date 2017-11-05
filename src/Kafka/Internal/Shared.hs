module Kafka.Internal.Shared
where

import           Control.Concurrent               (forkIO)
import           Control.Exception
import           Control.Monad                    (void)
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Internal         as BSI
import           Foreign.C.Error
import           Kafka.Internal.CancellationToken as CToken
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Setup
import           Kafka.Types

runEventLoop :: HasKafka a => a -> CancellationToken -> Maybe Timeout -> IO ()
runEventLoop k ct timeout = void $ forkIO go
  where
    go = do
      token <- CToken.status ct
      case token of
        Running   -> pollEvents k timeout >> go
        Cancelled -> return ()

pollEvents :: HasKafka a => a -> Maybe Timeout -> IO ()
pollEvents a tm =
  let timeout = maybe 0 (\(Timeout ms) -> ms) tm
      (Kafka k) = getKafka a
  in void (rdKafkaPoll k timeout)

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
        Just e  -> throw $ KafkaError e
        Nothing -> return ()

hasError :: KafkaError -> Bool
hasError err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> False
    _                                        -> True
{-# INLINE hasError #-}

rdKafkaErrorToEither :: RdKafkaRespErrT -> Either KafkaError ()
rdKafkaErrorToEither err = case err of
    RdKafkaRespErrNoError -> Right ()
    _                     -> Left (KafkaResponseError err)
{-# INLINE rdKafkaErrorToEither #-}

kafkaErrorToEither :: KafkaError -> Either KafkaError ()
kafkaErrorToEither err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> Right ()
    _                                        -> Left err
{-# INLINE kafkaErrorToEither #-}

kafkaErrorToMaybe :: KafkaError -> Maybe KafkaError
kafkaErrorToMaybe err = case err of
    KafkaResponseError RdKafkaRespErrNoError -> Nothing
    _                                        -> Just err
{-# INLINE kafkaErrorToMaybe #-}

maybeToLeft :: Maybe a -> Either a ()
maybeToLeft = maybe (Right ()) Left
{-# INLINE maybeToLeft #-}
