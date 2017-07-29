module Kafka.Consumer.Convert

where

import           Control.Monad
import qualified Data.ByteString        as BS
import           Data.Map.Strict        (Map, fromListWith)
import           Foreign
import           Foreign.C.Error
import           Foreign.C.String
import           Kafka.Consumer.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.Shared
import           Kafka.Types

-- | Converts offsets sync policy to integer (the way Kafka understands it):
--
--     * @OffsetSyncDisable == -1@
--
--     * @OffsetSyncImmediate == 0@
--
--     * @OffsetSyncInterval ms == ms@
offsetSyncToInt :: OffsetStoreSync -> Int
offsetSyncToInt sync =
    case sync of
        OffsetSyncDisable     -> -1
        OffsetSyncImmediate   -> 0
        OffsetSyncInterval ms -> ms
{-# INLINE offsetSyncToInt #-}

offsetToInt64 :: PartitionOffset -> Int64
offsetToInt64 o = case o of
    PartitionOffsetBeginning -> -2
    PartitionOffsetEnd       -> -1
    PartitionOffset off      -> off
    PartitionOffsetStored    -> -1000
    PartitionOffsetInvalid   -> -1001
{-# INLINE offsetToInt64 #-}

int64ToOffset :: Int64 -> PartitionOffset
int64ToOffset o
    | o == -2    = PartitionOffsetBeginning
    | o == -1    = PartitionOffsetEnd
    | o == -1000 = PartitionOffsetStored
    | o >= 0     = PartitionOffset o
    | otherwise  = PartitionOffsetInvalid
{-# INLINE int64ToOffset #-}

fromNativeTopicPartitionList'' :: RdKafkaTopicPartitionListTPtr -> IO [TopicPartition]
fromNativeTopicPartitionList'' ptr =
    withForeignPtr ptr $ \fptr -> fromNativeTopicPartitionList' fptr

fromNativeTopicPartitionList' :: Ptr RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList' ppl = peek ppl >>= fromNativeTopicPartitionList

fromNativeTopicPartitionList :: RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList pl =
    let count = cnt'RdKafkaTopicPartitionListT pl
        elems = elems'RdKafkaTopicPartitionListT pl
    in mapM (peekElemOff elems >=> toPart) [0..(fromIntegral count - 1)]
    where
        toPart :: RdKafkaTopicPartitionT -> IO TopicPartition
        toPart p = do
            topic <- peekCString $ topic'RdKafkaTopicPartitionT p
            return TopicPartition {
                tpTopicName = TopicName topic,
                tpPartition = PartitionId $ partition'RdKafkaTopicPartitionT p,
                tpOffset    = int64ToOffset $ offset'RdKafkaTopicPartitionT p
            }

toNativeTopicPartitionList :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
    pl <- newRdKafkaTopicPartitionListT (length ps)
    mapM_ (\p -> do
        let TopicName tn = tpTopicName p
            (PartitionId tp) = tpPartition p
            to = offsetToInt64 $ tpOffset p
        _ <- rdKafkaTopicPartitionListAdd pl tn tp
        rdKafkaTopicPartitionListSetOffset pl tn tp to) ps
    return pl

topicPartitionFromMessage :: ConsumerRecord k v -> TopicPartition
topicPartitionFromMessage m =
  let (Offset moff) = crOffset m
   in TopicPartition (crTopic m) (crPartition m) (PartitionOffset moff)

toMap :: Ord k => [(k, v)] -> Map k [v]
toMap kvs = fromListWith (++) [(k, [v]) | (k, v) <- kvs]

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
fromMessagePtr ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then (Left . kafkaRespErr) <$> getErrno
    else do
        s <- peek realPtr
        msg <- if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
                then return . Left . KafkaResponseError $ err'RdKafkaMessageT s
                else Right <$> mkRecord s
        rdKafkaMessageDestroy realPtr
        return msg
    where
        mkRecord msg = do
            topic     <- readTopic msg
            key       <- readKey msg
            payload   <- readPayload msg
            timestamp <- readTimestamp ptr
            return ConsumerRecord
                { crTopic     = TopicName topic
                , crPartition = PartitionId $ partition'RdKafkaMessageT msg
                , crOffset    = Offset $ offset'RdKafkaMessageT msg
                , crTimestamp = timestamp
                , crKey       = key
                , crValue     = payload
                }

        readTopic msg = newForeignPtr_ (topic'RdKafkaMessageT msg) >>= rdKafkaTopicName
        readPayload = readBS len'RdKafkaMessageT payload'RdKafkaMessageT
        readKey = readBS keyLen'RdKafkaMessageT key'RdKafkaMessageT
        readTimestamp msg =
            alloca $ \p -> do
                typeP <- newForeignPtr_ p
                ts <- fromIntegral <$> rdKafkaMessageTimestamp msg typeP
                tsType <- peek p
                return $ case tsType of
                    RdKafkaTimestampCreateTime    -> CreateTime (Millis ts)
                    RdKafkaTimestampLogAppendTime -> LogAppendTime (Millis ts)
                    RdKafkaTimestampNotAvailable  -> NoTimestamp

        readBS flen fdata s = if fdata s == nullPtr
                        then return Nothing
                        else Just <$> word8PtrToBS (flen s) (fdata s)


offsetCommitToBool :: OffsetCommit -> Bool
offsetCommitToBool OffsetCommit      = False
offsetCommitToBool OffsetCommitAsync = True
