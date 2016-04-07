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
                tpPartition = partition'RdKafkaTopicPartitionT p,
                tpOffset    = int64ToOffset $ offset'RdKafkaTopicPartitionT p
            }

toNativeTopicPartitionList :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
    pl <- newRdKafkaTopicPartitionListT (length ps)
    mapM_ (\p -> do
        let TopicName tn = tpTopicName p
            tp = tpPartition p
            to = offsetToInt64 $ tpOffset p
        _ <- rdKafkaTopicPartitionListAdd pl tn tp
        rdKafkaTopicPartitionListSetOffset pl tn tp to) ps
    return pl

topicPartitionFromMessage :: ReceivedMessage -> TopicPartition
topicPartitionFromMessage m =
    TopicPartition (TopicName $ messageTopic m) (messagePartition m) (PartitionOffset $ messageOffset m)

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError ReceivedMessage)
fromMessagePtr ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then liftM (Left . kafkaRespErr) getErrno
    else do
        s <- peek realPtr
        msg <- if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then return $ Left . KafkaResponseError $ err'RdKafkaMessageT s
            else Right <$> fromMessageStorable s
        rdKafkaMessageDestroy realPtr
        return msg

fromMessageStorable :: RdKafkaMessageT -> IO ReceivedMessage
fromMessageStorable s = do
    payload <- word8PtrToBS (len'RdKafkaMessageT s) (payload'RdKafkaMessageT s)
    topic   <- newForeignPtr_ (topic'RdKafkaMessageT s) >>= rdKafkaTopicName

    key <- if key'RdKafkaMessageT s == nullPtr
               then return Nothing
               else liftM Just $ word8PtrToBS (keyLen'RdKafkaMessageT s) (key'RdKafkaMessageT s)

    return $ ReceivedMessage
             topic
             (partition'RdKafkaMessageT s)
             (offset'RdKafkaMessageT s)
             payload
             key

offsetCommitToBool :: OffsetCommit -> Bool
offsetCommitToBool OffsetCommit = False
offsetCommitToBool OffsetCommitAsync = True
