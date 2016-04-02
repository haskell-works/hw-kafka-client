module Kafka.Consumer.Internal.Convert

where

import           Control.Monad
import           Foreign
import           Foreign.C.Error
import           Foreign.C.String
import           Kafka
import           Kafka.Consumer.Internal.Types
import           Kafka.Internal.RdKafka
import           Kafka.Internal.RdKafkaEnum
import           Kafka.Internal.Shared

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
        OffsetSyncDisable -> -1
        OffsetSyncImmediate -> 0
        OffsetSyncInterval ms -> ms
{-# INLINE offsetSyncToInt #-}

offsetToInt64 :: KafkaOffset -> Int64
offsetToInt64 o = case o of
    KafkaOffsetBeginning -> -2
    KafkaOffsetEnd       -> -1
    KafkaOffset off      -> off
    KafkaOffsetStored    -> -1000
    KafkaOffsetInvalid   -> -1001
{-# INLINE offsetToInt64 #-}

int64ToOffset :: Int64 -> KafkaOffset
int64ToOffset o
    | o == -2    = KafkaOffsetBeginning
    | o == -1    = KafkaOffsetEnd
    | o == -1000 = KafkaOffsetStored
    | o >= 0     = KafkaOffset o
    | otherwise  = KafkaOffsetInvalid
{-# INLINE int64ToOffset #-}

fromNativeTopicPartitionList :: RdKafkaTopicPartitionListT -> IO [KafkaTopicPartition]
fromNativeTopicPartitionList pl =
    let count = cnt'RdKafkaTopicPartitionListT pl
        elems = elems'RdKafkaTopicPartitionListT pl
    in mapM (peekElemOff elems >=> toPart) [0..(fromIntegral count - 1)]
    where
        toPart :: RdKafkaTopicPartitionT -> IO KafkaTopicPartition
        toPart p = do
            topic <- peekCString $ topic'RdKafkaTopicPartitionT p
            return KafkaTopicPartition {
                ktpTopicName = TopicName topic,
                ktpPartition = partition'RdKafkaTopicPartitionT p,
                ktpOffset    = int64ToOffset $ offset'RdKafkaTopicPartitionT p
            }

toNativeTopicPartitionList :: [KafkaTopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
    pl <- newRdKafkaTopicPartitionListT (length ps)
    mapM_ (\p -> do
        let TopicName tn = ktpTopicName p
            tp = ktpPartition p
            to = offsetToInt64 $ ktpOffset p
        _ <- rdKafkaTopicPartitionListAdd pl tn tp
        rdKafkaTopicPartitionListSetOffset pl tn tp to) ps
    return pl

topicPartitionFromMessage :: KafkaMessage -> KafkaTopicPartition
topicPartitionFromMessage m =
    KafkaTopicPartition (TopicName $ messageTopic m) (messagePartition m) (KafkaOffset $ messageOffset m)

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError KafkaMessage)
fromMessagePtr ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then liftM (Left . kafkaRespErr) getErrno
    else do
        addForeignPtrFinalizer rdKafkaMessageDestroy ptr
        s <- peek realPtr
        if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then return $ Left . KafkaResponseError $ err'RdKafkaMessageT s
            else Right <$> fromMessageStorable s

fromMessageStorable :: RdKafkaMessageT -> IO KafkaMessage
fromMessageStorable s = do
    payload <- word8PtrToBS (len'RdKafkaMessageT s) (payload'RdKafkaMessageT s)
    topic   <- newForeignPtr_ (topic'RdKafkaMessageT s) >>= rdKafkaTopicName

    key <- if key'RdKafkaMessageT s == nullPtr
               then return Nothing
               else liftM Just $ word8PtrToBS (keyLen'RdKafkaMessageT s) (key'RdKafkaMessageT s)

    return $ KafkaMessage
             topic
             (partition'RdKafkaMessageT s)
             (offset'RdKafkaMessageT s)
             payload
             key

offsetCommitToBool :: OffsetCommit -> Bool
offsetCommitToBool OffsetCommit = False
offsetCommitToBool OffsetCommitAsync = True
