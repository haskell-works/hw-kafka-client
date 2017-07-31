{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}

module Kafka.Internal.RdKafka where

--import Control.Applicative
import Control.Monad
import Data.Word
import Foreign
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import System.IO
import System.Posix.IO
import System.Posix.Types

#include "rdkafka.h"

type CInt64T = {#type int64_t #}
type CInt32T = {#type int32_t #}

{#pointer *FILE as CFilePtr -> CFile #}
{#pointer *size_t as CSizePtr -> CSize #}

type Word8Ptr = Ptr Word8
type CCharBufPointer  = Ptr CChar

{#enum rd_kafka_type_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_conf_res_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_resp_err_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_timestamp_type_t as ^ {underscoreToCase} deriving (Show, Eq) #}

type RdKafkaMsgFlag = Int
rdKafkaMsgFlagFree :: RdKafkaMsgFlag
rdKafkaMsgFlagFree = 0x1
rdKafkaMsgFlagCopy :: RdKafkaMsgFlag
rdKafkaMsgFlagCopy = 0x2

-- Number of bytes allocated for an error buffer
nErrorBytes ::  Int
nErrorBytes = 1024 * 8

-- Helper functions
{#fun pure rd_kafka_version as ^
    {} -> `Int' #}

{#fun pure rd_kafka_version_str as ^
    {} -> `String' #}

{#fun pure rd_kafka_err2str as ^
    {enumToCInt `RdKafkaRespErrT'} -> `String' #}

{#fun pure rd_kafka_errno2err as ^
    {`Int'} -> `RdKafkaRespErrT' cIntToEnum #}


kafkaErrnoString :: IO (String)
kafkaErrnoString = do
    (Errno num) <- getErrno
    return $ rdKafkaErr2str $ rdKafkaErrno2err (fromIntegral num)

-- Kafka Pointer Types
data RdKafkaConfT
{#pointer *rd_kafka_conf_t as RdKafkaConfTPtr foreign -> RdKafkaConfT #}

data RdKafkaTopicConfT
{#pointer *rd_kafka_topic_conf_t as RdKafkaTopicConfTPtr foreign -> RdKafkaTopicConfT #}

data RdKafkaT
{#pointer *rd_kafka_t as RdKafkaTPtr foreign -> RdKafkaT #}

data RdKafkaTopicPartitionT = RdKafkaTopicPartitionT
    { topic'RdKafkaTopicPartitionT :: CString
    , partition'RdKafkaTopicPartitionT :: Int
    , offset'RdKafkaTopicPartitionT :: Int64
    , metadata'RdKafkaTopicPartitionT :: Word8Ptr
    , metadataSize'RdKafkaTopicPartitionT :: Int
    , opaque'RdKafkaTopicPartitionT :: Word8Ptr
    , err'RdKafkaTopicPartitionT :: RdKafkaRespErrT
    } deriving (Show, Eq)

instance Storable RdKafkaTopicPartitionT where
    alignment _ = {#alignof rd_kafka_topic_partition_t#}
    sizeOf _ = {#sizeof rd_kafka_topic_partition_t#}
    peek p = RdKafkaTopicPartitionT
        <$> liftM id           ({#get rd_kafka_topic_partition_t->topic #} p)
        <*> liftM fromIntegral ({#get rd_kafka_topic_partition_t->partition #} p)
        <*> liftM fromIntegral ({#get rd_kafka_topic_partition_t->offset #} p)
        <*> liftM castPtr      ({#get rd_kafka_topic_partition_t->metadata #} p)
        <*> liftM fromIntegral ({#get rd_kafka_topic_partition_t->metadata_size #} p)
        <*> liftM castPtr      ({#get rd_kafka_topic_partition_t->opaque #} p)
        <*> liftM cIntToEnum   ({#get rd_kafka_topic_partition_t->err #} p)
    poke p x = do
        {#set rd_kafka_topic_partition_t.topic#}         p (id           $ topic'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.partition#}     p (fromIntegral $ partition'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.offset#}        p (fromIntegral $ offset'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.metadata#}      p (castPtr      $ metadata'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.metadata_size#} p (fromIntegral $ metadataSize'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.opaque#}        p (castPtr      $ opaque'RdKafkaTopicPartitionT x)
        {#set rd_kafka_topic_partition_t.err#}           p (enumToCInt   $ err'RdKafkaTopicPartitionT x)

{#pointer *rd_kafka_topic_partition_t as RdKafkaTopicPartitionTPtr foreign -> RdKafkaTopicPartitionT #}

data RdKafkaTopicPartitionListT = RdKafkaTopicPartitionListT
    { cnt'RdKafkaTopicPartitionListT   :: Int
    , size'RdKafkaTopicPartitionListT  :: Int
    , elems'RdKafkaTopicPartitionListT :: Ptr RdKafkaTopicPartitionT
    } deriving (Show, Eq)

{#pointer *rd_kafka_topic_partition_list_t as RdKafkaTopicPartitionListTPtr foreign -> RdKafkaTopicPartitionListT #}

instance Storable RdKafkaTopicPartitionListT where
  alignment _ = {#alignof rd_kafka_topic_partition_list_t#}
  sizeOf _    = {#sizeof  rd_kafka_topic_partition_list_t #}
  peek p = RdKafkaTopicPartitionListT
    <$> liftM fromIntegral ({#get rd_kafka_topic_partition_list_t->cnt #} p)
    <*> liftM fromIntegral ({#get rd_kafka_topic_partition_list_t->size #} p)
    <*> liftM castPtr      ({#get rd_kafka_topic_partition_list_t->elems #} p)
  poke p x = do
    {#set rd_kafka_topic_partition_list_t.cnt#}   p (fromIntegral $ cnt'RdKafkaTopicPartitionListT x)
    {#set rd_kafka_topic_partition_list_t.size#}  p (fromIntegral $ size'RdKafkaTopicPartitionListT x)
    {#set rd_kafka_topic_partition_list_t.elems#} p (castPtr      $ elems'RdKafkaTopicPartitionListT x)

data RdKafkaTopicT
{#pointer *rd_kafka_topic_t as RdKafkaTopicTPtr foreign -> RdKafkaTopicT #}

data RdKafkaMessageT = RdKafkaMessageT
    { err'RdKafkaMessageT :: RdKafkaRespErrT
    , topic'RdKafkaMessageT :: Ptr RdKafkaTopicT
    , partition'RdKafkaMessageT :: Int
    , len'RdKafkaMessageT :: Int
    , keyLen'RdKafkaMessageT :: Int
    , offset'RdKafkaMessageT :: Int64
    , payload'RdKafkaMessageT :: Word8Ptr
    , key'RdKafkaMessageT :: Word8Ptr
    }
    deriving (Show, Eq)

instance Storable RdKafkaMessageT where
    alignment _ = {#alignof rd_kafka_message_t#}
    sizeOf _ = {#sizeof rd_kafka_message_t#}
    peek p = RdKafkaMessageT
        <$> liftM cIntToEnum    ({#get rd_kafka_message_t->err #} p)
        <*> liftM castPtr       ({#get rd_kafka_message_t->rkt #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_message_t->partition #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_message_t->len #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_message_t->key_len #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_message_t->offset #} p)
        <*> liftM castPtr       ({#get rd_kafka_message_t->payload #} p)
        <*> liftM castPtr       ({#get rd_kafka_message_t->key #} p)
    poke p x = do
      {#set rd_kafka_message_t.err#}        p (enumToCInt   $ err'RdKafkaMessageT x)
      {#set rd_kafka_message_t.rkt#}        p (castPtr      $ topic'RdKafkaMessageT x)
      {#set rd_kafka_message_t.partition#}  p (fromIntegral $ partition'RdKafkaMessageT x)
      {#set rd_kafka_message_t.len#}        p (fromIntegral $ len'RdKafkaMessageT x)
      {#set rd_kafka_message_t.key_len#}    p (fromIntegral $ keyLen'RdKafkaMessageT x)
      {#set rd_kafka_message_t.offset#}     p (fromIntegral $ offset'RdKafkaMessageT x)
      {#set rd_kafka_message_t.payload#}    p (castPtr      $ payload'RdKafkaMessageT x)
      {#set rd_kafka_message_t.key#}        p (castPtr      $ key'RdKafkaMessageT x)

{#pointer *rd_kafka_message_t as RdKafkaMessageTPtr foreign -> RdKafkaMessageT #}

data RdKafkaMetadataBrokerT = RdKafkaMetadataBrokerT
  { id'RdKafkaMetadataBrokerT  :: Int
  , host'RdKafkaMetadataBrokerT :: CString
  , port'RdKafkaMetadataBrokerT :: Int
  } deriving (Show, Eq)

{#pointer *rd_kafka_metadata_broker_t as RdKafkaMetadataBrokerTPtr -> RdKafkaMetadataBrokerT #}


instance Storable RdKafkaMetadataBrokerT where
  alignment _ = {#alignof rd_kafka_metadata_broker_t#}
  sizeOf _ = {#sizeof rd_kafka_metadata_broker_t#}
  peek p = RdKafkaMetadataBrokerT
    <$> liftM fromIntegral ({#get rd_kafka_metadata_broker_t->id #} p)
    <*> liftM id ({#get rd_kafka_metadata_broker_t->host #} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_broker_t->port #} p)
  poke = undefined

data RdKafkaMetadataPartitionT = RdKafkaMetadataPartitionT
  { id'RdKafkaMetadataPartitionT :: Int
  , err'RdKafkaMetadataPartitionT :: RdKafkaRespErrT
  , leader'RdKafkaMetadataPartitionT :: Int
  , replicaCnt'RdKafkaMetadataPartitionT :: Int
  , replicas'RdKafkaMetadataPartitionT :: Ptr CInt32T
  , isrCnt'RdKafkaMetadataPartitionT :: Int
  , isrs'RdKafkaMetadataPartitionT :: Ptr CInt32T
  } deriving (Show, Eq)

instance Storable RdKafkaMetadataPartitionT where
  alignment _ = {#alignof rd_kafka_metadata_partition_t#}
  sizeOf _ = {#sizeof rd_kafka_metadata_partition_t#}
  peek p = RdKafkaMetadataPartitionT
    <$> liftM fromIntegral ({#get rd_kafka_metadata_partition_t->id#} p)
    <*> liftM cIntToEnum ({#get rd_kafka_metadata_partition_t->err#} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_partition_t->leader#} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_partition_t->replica_cnt#} p)
    <*> liftM castPtr ({#get rd_kafka_metadata_partition_t->replicas#} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_partition_t->isr_cnt#} p)
    <*> liftM castPtr ({#get rd_kafka_metadata_partition_t->isrs#} p)

  poke = undefined

{#pointer *rd_kafka_metadata_partition_t as RdKafkaMetadataPartitionTPtr -> RdKafkaMetadataPartitionT #}

data RdKafkaMetadataTopicT = RdKafkaMetadataTopicT
  { topic'RdKafkaMetadataTopicT :: CString
  , partitionCnt'RdKafkaMetadataTopicT :: Int
  , partitions'RdKafkaMetadataTopicT :: Ptr RdKafkaMetadataPartitionT
  , err'RdKafkaMetadataTopicT :: RdKafkaRespErrT
  } deriving (Show, Eq)

instance Storable RdKafkaMetadataTopicT where
  alignment _ = {#alignof rd_kafka_metadata_topic_t#}
  sizeOf _ = {#sizeof rd_kafka_metadata_topic_t #}
  peek p = RdKafkaMetadataTopicT
    <$> liftM id ({#get rd_kafka_metadata_topic_t->topic #} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_topic_t->partition_cnt #} p)
    <*> liftM castPtr ({#get rd_kafka_metadata_topic_t->partitions #} p)
    <*> liftM cIntToEnum ({#get rd_kafka_metadata_topic_t->err #} p)
  poke _ _ = undefined

{#pointer *rd_kafka_metadata_topic_t as RdKafkaMetadataTopicTPtr -> RdKafkaMetadataTopicT #}

data RdKafkaMetadataT = RdKafkaMetadataT
  { brokerCnt'RdKafkaMetadataT :: Int
  , brokers'RdKafkaMetadataT :: RdKafkaMetadataBrokerTPtr
  , topicCnt'RdKafkaMetadataT :: Int
  , topics'RdKafkaMetadataT :: RdKafkaMetadataTopicTPtr
  , origBrokerId'RdKafkaMetadataT :: CInt32T
  } deriving (Show, Eq)

instance Storable RdKafkaMetadataT where
  alignment _ = {#alignof rd_kafka_metadata_t#}
  sizeOf _ = {#sizeof rd_kafka_metadata_t#}
  peek p = RdKafkaMetadataT
    <$> liftM fromIntegral ({#get rd_kafka_metadata_t->broker_cnt #} p)
    <*> liftM castPtr ({#get rd_kafka_metadata_t->brokers #} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_t->topic_cnt #} p)
    <*> liftM castPtr ({#get rd_kafka_metadata_t->topics #} p)
    <*> liftM fromIntegral ({#get rd_kafka_metadata_t->orig_broker_id #} p)
  poke _ _ = undefined

{#pointer *rd_kafka_metadata_t as RdKafkaMetadataTPtr foreign -> RdKafkaMetadataT #}

-------------------------------------------------------------------------------------------------
---- Partitions
{#fun rd_kafka_topic_partition_list_new as ^
    {`Int'} -> `RdKafkaTopicPartitionListTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_partition_list_destroy"
    rdKafkaTopicPartitionListDestroy :: FinalizerPtr RdKafkaTopicPartitionListT

newRdKafkaTopicPartitionListT :: Int -> IO RdKafkaTopicPartitionListTPtr
newRdKafkaTopicPartitionListT size = do
    ret <- rdKafkaTopicPartitionListNew size
    addForeignPtrFinalizer rdKafkaTopicPartitionListDestroy ret
    return ret

{# fun rd_kafka_topic_partition_list_add as ^
    {`RdKafkaTopicPartitionListTPtr', `String', `Int'} -> `RdKafkaTopicPartitionTPtr' #}

{# fun rd_kafka_topic_partition_list_add_range as ^
    {`RdKafkaTopicPartitionListTPtr', `String', `Int', `Int'} -> `()' #}

{# fun rd_kafka_topic_partition_list_copy as ^
    {`RdKafkaTopicPartitionListTPtr'} -> `RdKafkaTopicPartitionListTPtr' #}

copyRdKafkaTopicPartitionList :: RdKafkaTopicPartitionListTPtr -> IO RdKafkaTopicPartitionListTPtr
copyRdKafkaTopicPartitionList pl = do
    cp <- rdKafkaTopicPartitionListCopy pl
    addForeignPtrFinalizer rdKafkaTopicPartitionListDestroy cp
    return cp

{# fun rd_kafka_topic_partition_list_set_offset as ^
    {`RdKafkaTopicPartitionListTPtr', `String', `Int', `Int64'}
    -> `RdKafkaRespErrT' cIntToEnum #}

---- Rebalance Callback
type RdRebalanceCallback' = Ptr RdKafkaT -> CInt -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()
type RdRebalanceCallback = Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Ptr Word8 -> IO ()

foreign import ccall safe "wrapper"
    mkRebalanceCallback :: RdRebalanceCallback' -> IO (FunPtr RdRebalanceCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_rebalance_cb"
     rdKafkaConfSetRebalanceCb' ::
       Ptr RdKafkaConfT
       -> FunPtr RdRebalanceCallback'
       -> IO ()

rdKafkaConfSetRebalanceCb :: RdKafkaConfTPtr -> RdRebalanceCallback -> IO ()
rdKafkaConfSetRebalanceCb conf cb = do
    cb' <- mkRebalanceCallback (\k e p o -> cb k (cIntToEnum e) p o)
    withForeignPtr conf $ \c -> rdKafkaConfSetRebalanceCb' c cb'
    return ()

---- Delivery Callback
type DeliveryCallback = Ptr RdKafkaT -> Ptr RdKafkaMessageT -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkDeliveryCallback :: DeliveryCallback -> IO (FunPtr DeliveryCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_dr_msg_cb"
    rdKafkaConfSetDrMsgCb' :: Ptr RdKafkaConfT -> FunPtr DeliveryCallback -> IO ()

rdKafkaConfSetDrMsgCb :: RdKafkaConfTPtr -> DeliveryCallback -> IO ()
rdKafkaConfSetDrMsgCb conf cb = do
    cb' <- mkDeliveryCallback cb
    withForeignPtr conf $ \c -> rdKafkaConfSetDrMsgCb' c cb'
    return ()

---- Consume Callback
type ConsumeCallback = Ptr RdKafkaMessageT -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkConsumeCallback :: ConsumeCallback -> IO (FunPtr ConsumeCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_consume_cb"
    rdKafkaConfSetConsumeCb' :: Ptr RdKafkaConfT -> FunPtr ConsumeCallback -> IO ()

rdKafkaConfSetConsumeCb :: RdKafkaConfTPtr -> ConsumeCallback -> IO ()
rdKafkaConfSetConsumeCb conf cb = do
    cb' <- mkConsumeCallback cb
    withForeignPtr conf $ \c -> rdKafkaConfSetConsumeCb' c cb'
    return ()

---- Offset Commit Callback
type OffsetCommitCallback' = Ptr RdKafkaT -> CInt -> Ptr RdKafkaTopicPartitionListT -> Word8Ptr -> IO ()
type OffsetCommitCallback  = Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkOffsetCommitCallback :: OffsetCommitCallback' -> IO (FunPtr OffsetCommitCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_offset_commit_cb"
    rdKafkaConfSetOffsetCommitCb' :: Ptr RdKafkaConfT -> FunPtr OffsetCommitCallback' -> IO ()

rdKafkaConfSetOffsetCommitCb :: RdKafkaConfTPtr -> OffsetCommitCallback -> IO ()
rdKafkaConfSetOffsetCommitCb conf cb = do
    cb' <- mkOffsetCommitCallback (\k e p o -> cb k (cIntToEnum e) p o)
    withForeignPtr conf $ \c -> rdKafkaConfSetOffsetCommitCb' c cb'
    return ()

---- Throttle Callback
type ThrottleCallback = Ptr RdKafkaT -> CString -> Int -> Int -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkThrottleCallback :: ThrottleCallback -> IO (FunPtr ThrottleCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_throttle_cb"
    rdKafkaConfSetThrottleCb' :: Ptr RdKafkaConfT -> FunPtr ThrottleCallback -> IO ()

rdKafkaConfSetThrottleCb :: RdKafkaConfTPtr -> ThrottleCallback -> IO ()
rdKafkaConfSetThrottleCb conf cb = do
    cb' <- mkThrottleCallback cb
    withForeignPtr conf $ \c -> rdKafkaConfSetThrottleCb' c cb'
    return ()

---- Stats Callback
type StatsCallback = Ptr RdKafkaT -> CString -> CSize -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkStatsCallback :: StatsCallback -> IO (FunPtr StatsCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_stats_cb"
    rdKafkaConfSetStatsCb' :: Ptr RdKafkaConfT -> FunPtr StatsCallback -> IO ()

rdKafkaConfSetStatsCb :: RdKafkaConfTPtr -> StatsCallback -> IO ()
rdKafkaConfSetStatsCb conf cb = do
    cb' <- mkStatsCallback cb
    withForeignPtr conf $ \c -> rdKafkaConfSetStatsCb' c cb'
    return ()

---- Socket Callback
type SocketCallback = Int -> Int -> Int -> Word8Ptr -> IO ()

foreign import ccall safe "wrapper"
    mkSocketCallback :: SocketCallback -> IO (FunPtr SocketCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_socket_cb"
    rdKafkaConfSetSocketCb' :: Ptr RdKafkaConfT -> FunPtr SocketCallback -> IO ()

rdKafkaConfSetSocketCb :: RdKafkaConfTPtr -> SocketCallback -> IO ()
rdKafkaConfSetSocketCb conf cb = do
    cb' <- mkSocketCallback cb
    withForeignPtr conf $ \c -> rdKafkaConfSetSocketCb' c cb'
    return ()

{#fun rd_kafka_conf_set_opaque as ^
    {`RdKafkaConfTPtr', castPtr `Word8Ptr'} -> `()' #}

{#fun rd_kafka_opaque as ^
    {`RdKafkaTPtr'} -> `Word8Ptr' castPtr #}

{#fun rd_kafka_conf_set_default_topic_conf as ^
   {`RdKafkaConfTPtr', `RdKafkaTopicConfTPtr'} -> `()' #}

---- Partitioner Callback
type PartitionerCallback =
    Ptr RdKafkaTopicTPtr
    -> Word8Ptr    -- keydata
    -> Int         -- keylen
    -> Int         -- partition_cnt
    -> Word8Ptr    -- topic_opaque
    -> Word8Ptr    -- msg_opaque
    -> IO Int

foreign import ccall safe "wrapper"
    mkPartitionerCallback :: PartitionerCallback -> IO (FunPtr PartitionerCallback)

foreign import ccall safe "rd_kafka.h rd_kafka_topic_conf_set_partitioner_cb"
    rdKafkaTopicConfSetPartitionerCb' :: Ptr RdKafkaTopicConfT -> FunPtr PartitionerCallback -> IO ()

rdKafkaTopicConfSetPartitionerCb :: RdKafkaTopicConfTPtr -> PartitionerCallback -> IO ()
rdKafkaTopicConfSetPartitionerCb conf cb = do
    cb' <- mkPartitionerCallback cb
    withForeignPtr conf $ \c -> rdKafkaTopicConfSetPartitionerCb' c cb'
    return ()

---- Partition

{#fun rd_kafka_topic_partition_available as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T'} -> `Int' #}

{#fun rd_kafka_msg_partitioner_random as ^
    { `RdKafkaTopicTPtr'
    , castPtr `Word8Ptr'
    , cIntConv `CSize'
    , cIntConv `CInt32T'
    , castPtr `Word8Ptr'
    , castPtr `Word8Ptr'}
    -> `CInt32T' cIntConv #}

{#fun rd_kafka_msg_partitioner_consistent as ^
    { `RdKafkaTopicTPtr'
    , castPtr `Word8Ptr'
    , cIntConv `CSize'
    , cIntConv `CInt32T'
    , castPtr `Word8Ptr'
    , castPtr `Word8Ptr'}
    -> `CInt32T' cIntConv #}

{#fun rd_kafka_msg_partitioner_consistent_random as ^
    { `RdKafkaTopicTPtr'
    , castPtr `Word8Ptr'
    , cIntConv `CSize'
    , cIntConv `CInt32T'
    , castPtr `Word8Ptr'
    , castPtr `Word8Ptr'}
    -> `CInt32T' cIntConv #}

---- Poll / Yield

{#fun rd_kafka_yield as ^
    {`RdKafkaTPtr'} -> `()' #}

---- Pause / Resume
{#fun rd_kafka_pause_partitions as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'} -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_resume_partitions as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'} -> `RdKafkaRespErrT' cIntToEnum #}

---- QUEUE
data RdKafkaQueueT
{#pointer *rd_kafka_queue_t as RdKafkaQueueTPtr foreign -> RdKafkaQueueT #}

{#fun rd_kafka_queue_new as ^
    {`RdKafkaTPtr'} -> `RdKafkaQueueTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_queue_destroy"
    rdKafkaQueueDestroy :: FinalizerPtr RdKafkaQueueT

newRdKafkaQueue :: RdKafkaTPtr -> IO RdKafkaQueueTPtr
newRdKafkaQueue k = do
    q <- rdKafkaQueueNew k
    addForeignPtrFinalizer rdKafkaQueueDestroy q
    return q
-------------------------------------------------------------------------------------------------
---- High-level KafkaConsumer

{#fun rd_kafka_subscribe as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_unsubscribe as ^
    {`RdKafkaTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_subscription as rdKafkaSubscription'
    {`RdKafkaTPtr', castPtr `Ptr (Ptr RdKafkaTopicPartitionListT)'}
    -> `RdKafkaRespErrT' cIntToEnum #}

rdKafkaSubscription :: RdKafkaTPtr -> IO (Either RdKafkaRespErrT RdKafkaTopicPartitionListTPtr)
rdKafkaSubscription k = alloca $ \psPtr -> do
    err <- rdKafkaSubscription' k psPtr
    case err of
        RdKafkaRespErrNoError -> do
            lst <- peek psPtr >>= newForeignPtr rdKafkaTopicPartitionListDestroy
            return (Right lst)
        e -> return (Left e)

{#fun rd_kafka_consumer_poll as ^
    {`RdKafkaTPtr', `Int'} -> `RdKafkaMessageTPtr' #}

pollRdKafkaConsumer :: RdKafkaTPtr -> Int -> IO RdKafkaMessageTPtr
pollRdKafkaConsumer k t = do
    m <- rdKafkaConsumerPoll k t
    addForeignPtrFinalizer rdKafkaMessageDestroyF m
    return m

{#fun rd_kafka_consumer_close as ^
    {`RdKafkaTPtr'} -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_poll_set_consumer as ^
    {`RdKafkaTPtr'} -> `RdKafkaRespErrT' cIntToEnum #}

-- rd_kafka_assign
{#fun rd_kafka_assign as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_assignment as rdKafkaAssignment'
    {`RdKafkaTPtr', castPtr `Ptr (Ptr RdKafkaTopicPartitionListT)'}
    -> `RdKafkaRespErrT' cIntToEnum #}

rdKafkaAssignment :: RdKafkaTPtr -> IO (Either RdKafkaRespErrT RdKafkaTopicPartitionListTPtr)
rdKafkaAssignment k = alloca $ \psPtr -> do
    err <- rdKafkaAssignment' k psPtr
    case err of
        RdKafkaRespErrNoError -> do
            lst <- peek psPtr >>= newForeignPtr rdKafkaTopicPartitionListDestroy
            return (Right lst)
        e -> return (Left e)

{#fun rd_kafka_commit as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr', boolToCInt `Bool'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_commit_message as ^
    {`RdKafkaTPtr', `RdKafkaMessageTPtr', boolToCInt `Bool'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_committed as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr', `Int'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_position as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

-------------------------------------------------------------------------------------------------
---- Groups
data RdKafkaGroupMemberInfoT = RdKafkaGroupMemberInfoT
    { memberId'RdKafkaGroupMemberInfoT              :: CString
    , clientId'RdKafkaGroupMemberInfoT              :: CString
    , clientHost'RdKafkaGroupMemberInfoT            :: CString
    , memberMetadata'RdKafkaGroupMemberInfoT        :: Word8Ptr
    , memberMetadataSize'RdKafkaGroupMemberInfoT    :: Int
    , memberAssignment'RdKafkaGroupMemberInfoT      :: Word8Ptr
    , memberAssignmentSize'RdKafkaGroupMemberInfoT  :: Int }

instance Storable RdKafkaGroupMemberInfoT where
    alignment _ = {#alignof rd_kafka_group_member_info#}
    sizeOf _ = {#sizeof rd_kafka_group_member_info#}
    peek p = RdKafkaGroupMemberInfoT
        <$> liftM id            ({#get rd_kafka_group_member_info->member_id #} p)
        <*> liftM id            ({#get rd_kafka_group_member_info->client_id #} p)
        <*> liftM id            ({#get rd_kafka_group_member_info->client_host #} p)
        <*> liftM castPtr       ({#get rd_kafka_group_member_info->member_metadata #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_group_member_info->member_metadata_size #} p)
        <*> liftM castPtr       ({#get rd_kafka_group_member_info->member_assignment #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_group_member_info->member_assignment_size #} p)
    poke p x = do
      {#set rd_kafka_group_member_info.member_id#}              p (id           $ memberId'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.client_id#}              p (id           $ clientId'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.client_host#}            p (id           $ clientHost'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.member_metadata#}        p (castPtr      $ memberMetadata'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.member_metadata_size#}   p (fromIntegral $ memberMetadataSize'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.member_assignment#}      p (castPtr      $ memberAssignment'RdKafkaGroupMemberInfoT x)
      {#set rd_kafka_group_member_info.member_assignment_size#} p (fromIntegral $ memberAssignmentSize'RdKafkaGroupMemberInfoT x)

{#pointer *rd_kafka_group_member_info as RdKafkaGroupMemberInfoTPtr -> RdKafkaGroupMemberInfoT #}

data RdKafkaGroupInfoT = RdKafkaGroupInfoT
    { broker'RdKafkaGroupInfoT       :: RdKafkaMetadataBrokerTPtr
    , group'RdKafkaGroupInfoT        :: CString
    , err'RdKafkaGroupInfoT          :: RdKafkaRespErrT
    , state'RdKafkaGroupInfoT        :: CString
    , protocolType'RdKafkaGroupInfoT :: CString
    , protocol'RdKafkaGroupInfoT     :: CString
    , members'RdKafkaGroupInfoT      :: RdKafkaGroupMemberInfoTPtr
    , memberCnt'RdKafkaGroupInfoT    :: Int }

instance Storable RdKafkaGroupInfoT where
    alignment _ = {#alignof rd_kafka_group_info #}
    sizeOf _ = {#sizeof rd_kafka_group_info #}
    peek p = RdKafkaGroupInfoT
        <$> liftM castPtr       ({#get rd_kafka_group_info->broker #} p)
        <*> liftM id            ({#get rd_kafka_group_info->group #} p)
        <*> liftM cIntToEnum    ({#get rd_kafka_group_info->err #} p)
        <*> liftM id            ({#get rd_kafka_group_info->state #} p)
        <*> liftM id            ({#get rd_kafka_group_info->protocol_type #} p)
        <*> liftM id            ({#get rd_kafka_group_info->protocol #} p)
        <*> liftM castPtr       ({#get rd_kafka_group_info->members #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_group_info->member_cnt #} p)
    poke p x = do
      {#set rd_kafka_group_info.broker#}        p (castPtr      $ broker'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.group#}         p (id           $ group'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.err#}           p (enumToCInt   $ err'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.state#}         p (id           $ state'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.protocol_type#} p (id           $ protocolType'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.protocol#}      p (id           $ protocol'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.members#}       p (castPtr      $ members'RdKafkaGroupInfoT x)
      {#set rd_kafka_group_info.member_cnt#}    p (fromIntegral $ memberCnt'RdKafkaGroupInfoT x)

{#pointer *rd_kafka_group_info as RdKafkaGroupInfoTPtr foreign -> RdKafkaGroupInfoT #}

data RdKafkaGroupListT = RdKafkaGroupListT
    { groups'RdKafkaGroupListT   :: Ptr RdKafkaGroupInfoT
    , groupCnt'RdKafkaGroupListT :: Int }

instance Storable RdKafkaGroupListT where
    alignment _ = {#alignof rd_kafka_group_list #}
    sizeOf _ = {#sizeof rd_kafka_group_list #}
    peek p = RdKafkaGroupListT
        <$> liftM castPtr       ({#get rd_kafka_group_list->groups #} p)
        <*> liftM fromIntegral  ({#get rd_kafka_group_list->group_cnt #} p)
    poke p x = do
      {#set rd_kafka_group_list.groups#}        p (castPtr      $ groups'RdKafkaGroupListT x)
      {#set rd_kafka_group_list.group_cnt#}     p (fromIntegral $ groupCnt'RdKafkaGroupListT x)

{#pointer *rd_kafka_group_list as RdKafkaGroupListTPtr foreign -> RdKafkaGroupListT #}

{#fun rd_kafka_list_groups as rdKafkaListGroups'
    {`RdKafkaTPtr', `CString', castPtr `Ptr (Ptr RdKafkaGroupListT)', `Int'}
    -> `RdKafkaRespErrT' cIntToEnum #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_list_groups"
    rdKafkaGroupListDestroy :: FinalizerPtr RdKafkaGroupListT

rdKafkaListGroups :: RdKafkaTPtr -> Maybe String -> Int -> IO (Either RdKafkaRespErrT RdKafkaGroupListTPtr)
rdKafkaListGroups k g t = case g of
    Nothing -> listGroups nullPtr
    Just strGrp -> withCAString strGrp listGroups
    where
        listGroups grp = alloca $ \lstDblPtr -> do
            err <- rdKafkaListGroups' k grp lstDblPtr t
            case err of
                RdKafkaRespErrNoError -> do
                    lstPtr <- peek lstDblPtr >>= newForeignPtr rdKafkaGroupListDestroy
                    return $ Right lstPtr
                e -> return $ Left e
-------------------------------------------------------------------------------------------------

-- rd_kafka_message
foreign import ccall unsafe "rdkafka.h &rd_kafka_message_destroy"
    rdKafkaMessageDestroyF :: FinalizerPtr RdKafkaMessageT

foreign import ccall unsafe "rdkafka.h rd_kafka_message_destroy"
    rdKafkaMessageDestroy :: Ptr RdKafkaMessageT -> IO ()

{#fun rd_kafka_query_watermark_offsets as rdKafkaQueryWatermarkOffsets'
    {`RdKafkaTPtr', `String', cIntConv `CInt32T',
      alloca- `Int64' peekInt64Conv*, alloca- `Int64' peekInt64Conv*,
      cIntConv `Int'
      } -> `RdKafkaRespErrT' cIntToEnum #}


rdKafkaQueryWatermarkOffsets :: RdKafkaTPtr -> String -> Int -> Int -> IO (Either RdKafkaRespErrT (Int64, Int64))
rdKafkaQueryWatermarkOffsets kafka topic partition timeout = do
    (err, l, h) <- rdKafkaQueryWatermarkOffsets' kafka topic (cIntConv partition) timeout
    return $ case err of
                RdKafkaRespErrNoError -> Right (cIntConv l, cIntConv h)
                e                     -> Left e

{#pointer *rd_kafka_timestamp_type_t as RdKafkaTimestampTypeTPtr foreign -> RdKafkaTimestampTypeT #}

instance Storable RdKafkaTimestampTypeT where
  sizeOf _    = {#sizeof rd_kafka_timestamp_type_t#}
  alignment _ = {#alignof rd_kafka_timestamp_type_t#}
  peek p      = cIntToEnum <$> peek (castPtr p)
  poke p x    = poke (castPtr p) (enumToCInt x)

{#fun rd_kafka_message_timestamp as ^
    {`RdKafkaMessageTPtr', `RdKafkaTimestampTypeTPtr'} -> `CInt64T' cIntConv #}

{#fun rd_kafka_offsets_for_times as rdKafkaOffsetsForTimes
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr', `Int'} -> `RdKafkaRespErrT' cIntToEnum #}

-- rd_kafka_conf
{#fun rd_kafka_conf_new as ^
    {} -> `RdKafkaConfTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_conf_destroy"
    rdKafkaConfDestroy :: FinalizerPtr RdKafkaConfT

{#fun rd_kafka_conf_dup as ^
    {`RdKafkaConfTPtr'} -> `RdKafkaConfTPtr' #}

{#fun rd_kafka_conf_set as ^
  {`RdKafkaConfTPtr', `String', `String', id `CCharBufPointer', cIntConv `CSize'}
  -> `RdKafkaConfResT' cIntToEnum #}

newRdKafkaConfT :: IO RdKafkaConfTPtr
newRdKafkaConfT = do
    ret <- rdKafkaConfNew
    addForeignPtrFinalizer rdKafkaConfDestroy ret
    return ret

{#fun rd_kafka_conf_dump as ^
    {`RdKafkaConfTPtr', castPtr `CSizePtr'} -> `Ptr CString' id #}

{#fun rd_kafka_conf_dump_free as ^
    {id `Ptr CString', cIntConv `CSize'} -> `()' #}

{#fun rd_kafka_conf_properties_show as ^
    {`CFilePtr'} -> `()' #}

-- rd_kafka_topic_conf
{#fun rd_kafka_topic_conf_new as ^
    {} -> `RdKafkaTopicConfTPtr' #}

{#fun rd_kafka_topic_conf_dup as ^
    {`RdKafkaTopicConfTPtr'} -> `RdKafkaTopicConfTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_conf_destroy"
    rdKafkaTopicConfDestroy :: FinalizerPtr RdKafkaTopicConfT

{#fun rd_kafka_topic_conf_set as ^
  {`RdKafkaTopicConfTPtr', `String', `String', id `CCharBufPointer', cIntConv `CSize'}
  -> `RdKafkaConfResT' cIntToEnum #}

newRdKafkaTopicConfT :: IO RdKafkaTopicConfTPtr
newRdKafkaTopicConfT = do
    ret <- rdKafkaTopicConfNew
    addForeignPtrFinalizer rdKafkaTopicConfDestroy ret
    return ret

{#fun rd_kafka_topic_conf_dump as ^
    {`RdKafkaTopicConfTPtr', castPtr `CSizePtr'} -> `Ptr CString' id #}

-- rd_kafka
{#fun rd_kafka_new as ^
    {enumToCInt `RdKafkaTypeT', `RdKafkaConfTPtr', id `CCharBufPointer', cIntConv `CSize'}
    -> `RdKafkaTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_destroy"
    rdKafkaDestroy :: FunPtr (Ptr RdKafkaT -> IO ())

newRdKafkaT :: RdKafkaTypeT -> RdKafkaConfTPtr -> IO (Either String RdKafkaTPtr)
newRdKafkaT kafkaType confPtr =
    allocaBytes nErrorBytes $ \charPtr -> do
        duper <- rdKafkaConfDup confPtr
        ret <- rdKafkaNew kafkaType duper charPtr (fromIntegral nErrorBytes)
        withForeignPtr ret $ \realPtr -> do
            if realPtr == nullPtr then peekCString charPtr >>= return . Left
            else do
                addForeignPtrFinalizer rdKafkaDestroy ret
                return $ Right ret

{#fun rd_kafka_brokers_add as ^
    {`RdKafkaTPtr', `String'} -> `Int' #}

{#fun rd_kafka_set_log_level as ^
  {`RdKafkaTPtr', `Int'} -> `()' #}

-- rd_kafka consume

{#fun rd_kafka_consume_start as rdKafkaConsumeStartInternal
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', cIntConv `CInt64T'} -> `Int' #}

rdKafkaConsumeStart :: RdKafkaTopicTPtr -> Int -> Int64 -> IO (Maybe String)
rdKafkaConsumeStart topicPtr partition offset = do
    i <- rdKafkaConsumeStartInternal topicPtr (fromIntegral partition) (fromIntegral offset)
    case i of
        -1 -> kafkaErrnoString >>= return . Just
        _ -> return Nothing
{#fun rd_kafka_consume_stop as rdKafkaConsumeStopInternal
    {`RdKafkaTopicTPtr', cIntConv `CInt32T'} -> `Int' #}

{#fun rd_kafka_consume as ^
  {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int'} -> `RdKafkaMessageTPtr' #}

{#fun rd_kafka_consume_batch as ^
  {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int', castPtr `Ptr (Ptr RdKafkaMessageT)', cIntConv `CSize'}
  -> `CSize' cIntConv #}

rdKafkaConsumeStop :: RdKafkaTopicTPtr -> Int -> IO (Maybe String)
rdKafkaConsumeStop topicPtr partition = do
    i <- rdKafkaConsumeStopInternal topicPtr (fromIntegral partition)
    case i of
        -1 -> kafkaErrnoString >>= return . Just
        _ -> return Nothing

{#fun rd_kafka_offset_store as rdKafkaOffsetStore
  {`RdKafkaTopicTPtr', cIntConv `CInt32T', cIntConv `CInt64T'}
  -> `RdKafkaRespErrT' cIntToEnum #}

-- rd_kafka produce

{#fun rd_kafka_produce as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int', castPtr `Word8Ptr',
     cIntConv `CSize', castPtr `Word8Ptr', cIntConv `CSize', castPtr `Word8Ptr'}
     -> `Int' #}

{#fun rd_kafka_produce_batch as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int', `RdKafkaMessageTPtr', `Int'} -> `Int' #}

castMetadata :: Ptr (Ptr RdKafkaMetadataT) -> Ptr (Ptr ())
castMetadata ptr = castPtr ptr

-- rd_kafka_metadata

{#fun rd_kafka_metadata as rdKafkaMetadata'
   {`RdKafkaTPtr', boolToCInt `Bool', `RdKafkaTopicTPtr',
    castMetadata `Ptr (Ptr RdKafkaMetadataT)', `Int'}
   -> `RdKafkaRespErrT' cIntToEnum #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_metadata_destroy"
    rdKafkaMetadataDestroy :: FinalizerPtr RdKafkaMetadataT

rdKafkaMetadata :: RdKafkaTPtr -> Bool -> Maybe RdKafkaTopicTPtr -> IO (Either RdKafkaRespErrT RdKafkaMetadataTPtr)
rdKafkaMetadata k allTopics mt = alloca $ \mptr -> do
    tptr <- maybe (newForeignPtr_ nullPtr) pure mt
    err <- rdKafkaMetadata' k allTopics tptr mptr (-1)
    case err of
        RdKafkaRespErrNoError -> do
            meta <- peek mptr >>= newForeignPtr rdKafkaMetadataDestroy
            return (Right meta)
        e -> return (Left e)

{#fun rd_kafka_poll as ^
    {`RdKafkaTPtr', `Int'} -> `Int' #}

{#fun rd_kafka_outq_len as ^
    {`RdKafkaTPtr'} -> `Int' #}

{#fun rd_kafka_dump as ^
    {`CFilePtr', `RdKafkaTPtr'} -> `()' #}


-- rd_kafka_topic
{#fun rd_kafka_topic_name as ^
    {`RdKafkaTopicTPtr'} -> `String' #}

{#fun rd_kafka_topic_new as ^
    {`RdKafkaTPtr', `String', `RdKafkaTopicConfTPtr'} -> `RdKafkaTopicTPtr' #}

{# fun rd_kafka_topic_destroy as ^
   {castPtr `Ptr RdKafkaTopicT'} -> `()' #}

destroyUnmanagedRdKafkaTopic :: RdKafkaTopicTPtr -> IO ()
destroyUnmanagedRdKafkaTopic ptr =
  withForeignPtr ptr rdKafkaTopicDestroy

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_destroy"
    rdKafkaTopicDestroy' :: FinalizerPtr RdKafkaTopicT

newUnmanagedRdKafkaTopicT :: RdKafkaTPtr -> String -> RdKafkaTopicConfTPtr -> IO (Either String RdKafkaTopicTPtr)
newUnmanagedRdKafkaTopicT kafkaPtr topic topicConfPtr = do
    duper <- rdKafkaTopicConfDup topicConfPtr
    ret <- rdKafkaTopicNew kafkaPtr topic duper
    withForeignPtr ret $ \realPtr ->
        if realPtr == nullPtr then kafkaErrnoString >>= return . Left
        else return $ Right ret

newRdKafkaTopicT :: RdKafkaTPtr -> String -> RdKafkaTopicConfTPtr -> IO (Either String RdKafkaTopicTPtr)
newRdKafkaTopicT kafkaPtr topic topicConfPtr = do
    duper <- rdKafkaTopicConfDup topicConfPtr
    ret <- rdKafkaTopicNew kafkaPtr topic duper
    withForeignPtr ret $ \realPtr ->
        if realPtr == nullPtr then kafkaErrnoString >>= return . Left
        else do
            addForeignPtrFinalizer rdKafkaTopicDestroy' ret
            return $ Right ret

-- Marshall / Unmarshall
enumToCInt :: Enum a => a -> CInt
enumToCInt = fromIntegral . fromEnum
{-# INLINE enumToCInt #-}

cIntToEnum :: Enum a => CInt -> a
cIntToEnum = toEnum . fromIntegral
{-# INLINE cIntToEnum #-}

cIntConv :: (Integral a, Num b) =>  a -> b
cIntConv = fromIntegral
{-# INLINE cIntConv #-}

boolToCInt :: Bool -> CInt
boolToCInt True = CInt 1
boolToCInt False = CInt 0
{-# INLINE boolToCInt #-}

peekInt64Conv :: (Storable a, Integral a) =>  Ptr a -> IO Int64
peekInt64Conv  = liftM cIntConv . peek
{-# INLINE peekInt64Conv #-}

-- Handle -> File descriptor

foreign import ccall "" fdopen :: Fd -> CString -> IO (Ptr CFile)

handleToCFile :: Handle -> String -> IO (CFilePtr)
handleToCFile h m =
 do iomode <- newCString m
    fd <- handleToFd h
    fdopen fd iomode

c_stdin :: IO CFilePtr
c_stdin = handleToCFile stdin "r"
c_stdout :: IO CFilePtr
c_stdout = handleToCFile stdout "w"
c_stderr :: IO CFilePtr
c_stderr = handleToCFile stderr "w"
