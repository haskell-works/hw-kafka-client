{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}

module Kafka.Internal.RdKafka where

import Data.Text (Text)
import qualified Data.Text as Text
import Control.Monad (liftM)
import Data.Int (Int32, Int64)
import Data.Word (Word8)
import Foreign.Marshal.Alloc (alloca, allocaBytes)
import Foreign.Marshal.Array (peekArray, allocaArray, withArrayLen)
import Foreign.Storable (Storable(..))
import Foreign.Ptr (Ptr, FunPtr, castPtr, nullPtr)
import Foreign.ForeignPtr (ForeignPtr, FinalizerPtr, addForeignPtrFinalizer, withForeignPtr, newForeignPtr, newForeignPtr_)
import Foreign.C.Error (Errno(..), getErrno)
import Foreign.C.String (CString, newCString, withCAString, withCString, peekCAString, peekCAStringLen, peekCString)
import Foreign.C.Types (CFile, CInt(..), CSize, CChar)
import System.IO (Handle, stdin, stdout, stderr)
import System.Posix.IO (handleToFd)
import System.Posix.Types (Fd(..))

#include <librdkafka/rdkafka.h>

type CInt64T = {#type int64_t #}
type CInt32T = {#type int32_t #}

{#pointer *FILE as CFilePtr -> CFile #}
{#pointer *size_t as CSizePtr -> CSize #}

type Word8Ptr = Ptr Word8
type CCharBufPointer  = Ptr CChar

{#enum rd_kafka_type_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_conf_res_t as ^ {underscoreToCase} deriving (Show, Eq) #}
{#enum rd_kafka_resp_err_t as ^ {underscoreToCase} deriving (Show, Eq, Bounded) #}
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

{#fun pure rd_kafka_err2name as ^
    {enumToCInt `RdKafkaRespErrT'} -> `String' #}

{#fun pure rd_kafka_errno2err as ^
    {`Int'} -> `RdKafkaRespErrT' cIntToEnum #}

peekCAText :: CString -> IO Text
peekCAText cp = Text.pack <$> peekCAString cp

peekCText :: CString -> IO Text
peekCText cp = Text.pack <$> peekCString cp

kafkaErrnoString :: IO String
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

data RdKafkaEventT
{#pointer *rd_kafka_event_t as RdKafkaEventTPtr foreign -> RdKafkaEventT #}

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
---- Events

foreign import ccall unsafe "rdkafka.h &rd_kafka_event_destroy"
    rdKafkaEventDestroyF :: FinalizerPtr RdKafkaEventT

{#fun rd_kafka_event_destroy as ^
    {`RdKafkaEventTPtr'} -> `()'#}

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
type RdRebalanceCallback = Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> IO ()

foreign import ccall safe "wrapper"
    mkRebalanceCallback :: RdRebalanceCallback' -> IO (FunPtr RdRebalanceCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_rebalance_cb"
     rdKafkaConfSetRebalanceCb' ::
       Ptr RdKafkaConfT
       -> FunPtr RdRebalanceCallback'
       -> IO ()

rdKafkaConfSetRebalanceCb :: RdKafkaConfTPtr -> RdRebalanceCallback -> IO ()
rdKafkaConfSetRebalanceCb conf cb = do
    cb' <- mkRebalanceCallback (\k e p _ -> cb k (cIntToEnum e) p)
    withForeignPtr conf $ \c -> rdKafkaConfSetRebalanceCb' c cb'
    return ()

---- Delivery Callback
type DeliveryCallback' = Ptr RdKafkaT -> Ptr RdKafkaMessageT -> Word8Ptr -> IO ()
type DeliveryCallback = Ptr RdKafkaT -> Ptr RdKafkaMessageT -> IO ()

foreign import ccall safe "wrapper"
    mkDeliveryCallback :: DeliveryCallback' -> IO (FunPtr DeliveryCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_dr_msg_cb"
    rdKafkaConfSetDrMsgCb' :: Ptr RdKafkaConfT -> FunPtr DeliveryCallback' -> IO ()

rdKafkaConfSetDrMsgCb :: RdKafkaConfTPtr -> DeliveryCallback -> IO ()
rdKafkaConfSetDrMsgCb conf cb = do
    cb' <- mkDeliveryCallback (\k m _ -> cb k m)
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
type OffsetCommitCallback  = Ptr RdKafkaT -> RdKafkaRespErrT -> Ptr RdKafkaTopicPartitionListT -> IO ()

foreign import ccall safe "wrapper"
    mkOffsetCommitCallback :: OffsetCommitCallback' -> IO (FunPtr OffsetCommitCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_offset_commit_cb"
    rdKafkaConfSetOffsetCommitCb' :: Ptr RdKafkaConfT -> FunPtr OffsetCommitCallback' -> IO ()

rdKafkaConfSetOffsetCommitCb :: RdKafkaConfTPtr -> OffsetCommitCallback -> IO ()
rdKafkaConfSetOffsetCommitCb conf cb = do
    cb' <- mkOffsetCommitCallback (\k e p _ -> cb k (cIntToEnum e) p)
    withForeignPtr conf $ \c -> rdKafkaConfSetOffsetCommitCb' c cb'
    return ()


----- Error Callback
type ErrorCallback' = Ptr RdKafkaT -> CInt -> CString -> Word8Ptr -> IO ()
type ErrorCallback  = Ptr RdKafkaT -> RdKafkaRespErrT -> String -> IO ()

foreign import ccall safe "wrapper"
    mkErrorCallback :: ErrorCallback' -> IO (FunPtr ErrorCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_error_cb"
    rdKafkaConfSetErrorCb' :: Ptr RdKafkaConfT -> FunPtr ErrorCallback' -> IO ()

rdKafkaConfSetErrorCb :: RdKafkaConfTPtr -> ErrorCallback -> IO ()
rdKafkaConfSetErrorCb conf cb = do
    cb' <- mkErrorCallback (\k e r _ -> peekCAString r >>= cb k (cIntToEnum e))
    withForeignPtr conf $ \c -> rdKafkaConfSetErrorCb' c cb'

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

---- Log Callback
type LogCallback' = Ptr RdKafkaT -> CInt -> CString -> CString -> IO ()
type LogCallback = Ptr RdKafkaT -> Int -> String -> String -> IO ()

foreign import ccall safe "wrapper"
    mkLogCallback :: LogCallback' -> IO (FunPtr LogCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_log_cb"
    rdKafkaConfSetLogCb' :: Ptr RdKafkaConfT -> FunPtr LogCallback' -> IO ()

rdKafkaConfSetLogCb :: RdKafkaConfTPtr -> LogCallback -> IO ()
rdKafkaConfSetLogCb conf cb = do
    cb' <- mkLogCallback $ \k l f b -> do
            f' <- peekCAString f
            b' <- peekCAString b
            cb k (cIntConv l) f' b'
    withForeignPtr conf $ \c -> rdKafkaConfSetLogCb' c cb'

---- Stats Callback
type StatsCallback' = Ptr RdKafkaT -> CString -> CSize -> Word8Ptr -> IO ()
type StatsCallback = Ptr RdKafkaT -> String -> IO ()

foreign import ccall safe "wrapper"
    mkStatsCallback :: StatsCallback' -> IO (FunPtr StatsCallback')

foreign import ccall safe "rd_kafka.h rd_kafka_conf_set_stats_cb"
    rdKafkaConfSetStatsCb' :: Ptr RdKafkaConfT -> FunPtr StatsCallback' -> IO ()

rdKafkaConfSetStatsCb :: RdKafkaConfTPtr -> StatsCallback -> IO ()
rdKafkaConfSetStatsCb conf cb = do
    cb' <- mkStatsCallback $ \k j jl _ -> peekCAStringLen (j, cIntConv jl) >>= cb k
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
    rdKafkaQueueDestroyF :: FinalizerPtr RdKafkaQueueT

{#fun rd_kafka_queue_destroy as ^
    {`RdKafkaQueueTPtr'} -> `()'#}

newRdKafkaQueue :: RdKafkaTPtr -> IO RdKafkaQueueTPtr
newRdKafkaQueue k = do
    q <- rdKafkaQueueNew k
    addForeignPtrFinalizer rdKafkaQueueDestroyF q
    return q

rdKafkaQueuePoll :: RdKafkaQueueTPtr -> Int -> IO (Maybe RdKafkaEventTPtr)
rdKafkaQueuePoll qPtr timeout =
  withForeignPtr qPtr $ \qPtr' -> do
    res <- {#call rd_kafka_queue_poll#} qPtr' (fromIntegral timeout)
    if res == nullPtr
      then pure Nothing
      else Just <$> newForeignPtr rdKafkaEventDestroyF res

{#fun rd_kafka_consume_queue as ^
    {`RdKafkaQueueTPtr', `Int'} -> `RdKafkaMessageTPtr' #}

{#fun rd_kafka_queue_forward as ^
    {`RdKafkaQueueTPtr', `RdKafkaQueueTPtr'} -> `()' #}

{#fun rd_kafka_queue_get_partition as rdKafkaQueueGetPartition'
    {`RdKafkaTPtr', `String', `Int'} -> `RdKafkaQueueTPtr' #}

rdKafkaQueueGetPartition :: RdKafkaTPtr -> String -> Int -> IO (Maybe RdKafkaQueueTPtr)
rdKafkaQueueGetPartition k t p = do
    ret <- rdKafkaQueueGetPartition' k t p
    withForeignPtr ret $ \realPtr ->
        if realPtr == nullPtr then return Nothing
        else do
            addForeignPtrFinalizer rdKafkaQueueDestroyF ret
            return $ Just ret

{#fun rd_kafka_consume_batch_queue as rdKafkaConsumeBatchQueue'
  {`RdKafkaQueueTPtr', `Int', castPtr `Ptr (Ptr RdKafkaMessageT)', cIntConv `CSize'}
  -> `CSize' cIntConv #}

rdKafkaConsumeBatchQueue :: RdKafkaQueueTPtr -> Int -> Int -> IO [RdKafkaMessageTPtr]
rdKafkaConsumeBatchQueue qptr timeout batchSize = do
  allocaArray batchSize $ \pArr -> do
    rSize <- rdKafkaConsumeBatchQueue' qptr timeout pArr (fromIntegral batchSize)
    peekArray (fromIntegral rSize) pArr >>= traverse newForeignPtr_

-------------------------------------------------------------------------------------------------
---- High-level KafkaConsumer

{#fun rd_kafka_subscribe as ^
    {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_unsubscribe as ^
    {`RdKafkaTPtr'}
    -> `RdKafkaRespErrT' cIntToEnum #}

{#fun rd_kafka_subscription as rdKafkaSubscription'
    {`RdKafkaTPtr', alloca- `Ptr RdKafkaTopicPartitionListT' peekPtr*}
    -> `RdKafkaRespErrT' cIntToEnum #}

rdKafkaSubscription :: RdKafkaTPtr -> IO (Either RdKafkaRespErrT RdKafkaTopicPartitionListTPtr)
rdKafkaSubscription k = do
    (err, sub) <- rdKafkaSubscription' k
    case err of
        RdKafkaRespErrNoError ->
            Right <$> newForeignPtr rdKafkaTopicPartitionListDestroy sub
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
    {`RdKafkaTPtr', alloca- `Ptr RdKafkaTopicPartitionListT' peekPtr* }
    -> `RdKafkaRespErrT' cIntToEnum #}

rdKafkaAssignment :: RdKafkaTPtr -> IO (Either RdKafkaRespErrT RdKafkaTopicPartitionListTPtr)
rdKafkaAssignment k = do
    (err, ass) <- rdKafkaAssignment' k
    case err of
        RdKafkaRespErrNoError ->
            Right <$> newForeignPtr rdKafkaTopicPartitionListDestroy ass
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
    {`RdKafkaTPtr', `CString', alloca- `Ptr RdKafkaGroupListT' peek*, `Int'}
    -> `RdKafkaRespErrT' cIntToEnum #}

foreign import ccall "rdkafka.h &rd_kafka_group_list_destroy"
    rdKafkaGroupListDestroy :: FinalizerPtr RdKafkaGroupListT

rdKafkaListGroups :: RdKafkaTPtr -> Maybe String -> Int -> IO (Either RdKafkaRespErrT RdKafkaGroupListTPtr)
rdKafkaListGroups k g t = case g of
    Nothing -> listGroups nullPtr
    Just strGrp -> withCAString strGrp listGroups
    where
        listGroups grp = do
            (err, res) <- rdKafkaListGroups' k grp t
            case err of
                RdKafkaRespErrNoError -> Right <$> newForeignPtr rdKafkaGroupListDestroy res
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

{#fun rd_kafka_message_timestamp as rdKafkaReadTimestamp'
    {castPtr `Ptr RdKafkaMessageT', `RdKafkaTimestampTypeTPtr'} -> `CInt64T' cIntConv #}

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

foreign import ccall safe "rdkafka.h &rd_kafka_destroy"
    rdKafkaDestroy :: FunPtr (Ptr RdKafkaT -> IO ())

newRdKafkaT :: RdKafkaTypeT -> RdKafkaConfTPtr -> IO (Either Text RdKafkaTPtr)
newRdKafkaT kafkaType confPtr =
    allocaBytes nErrorBytes $ \charPtr -> do
        duper <- rdKafkaConfDup confPtr
        ret <- rdKafkaNew kafkaType duper charPtr (fromIntegral nErrorBytes)
        withForeignPtr ret $ \realPtr -> do
            if realPtr == nullPtr then peekCText charPtr >>= return . Left
            else do
                -- do not call 'rd_kafka_close_consumer' on destroying all Kafka.
                -- when needed, applications should do it explicitly.
                -- {# call rd_kafka_destroy_flags #} realPtr 0x8
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

{#fun rd_kafka_seek as rdKafkaSeek
    {`RdKafkaTopicTPtr', `Int32', `Int64', `Int'} -> `RdKafkaRespErrT' cIntToEnum #}

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

{#fun rd_kafka_offsets_store as rdKafkaOffsetsStore
  {`RdKafkaTPtr', `RdKafkaTopicPartitionListTPtr'}
  -> `RdKafkaRespErrT' cIntToEnum #}

-- rd_kafka produce

{#fun rd_kafka_produce as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int', castPtr `Word8Ptr',
     cIntConv `CSize', castPtr `Word8Ptr', cIntConv `CSize', castPtr `Word8Ptr'}
     -> `Int' #}

{#fun rd_kafka_produce_batch as ^
    {`RdKafkaTopicTPtr', cIntConv `CInt32T', `Int', `RdKafkaMessageTPtr', `Int'} -> `Int' #}


-- rd_kafka_metadata

{#fun rd_kafka_metadata as rdKafkaMetadata'
   {`RdKafkaTPtr', boolToCInt `Bool', `RdKafkaTopicTPtr',
    alloca- `Ptr RdKafkaMetadataT' peekPtr*, `Int'}
   -> `RdKafkaRespErrT' cIntToEnum #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_metadata_destroy"
    rdKafkaMetadataDestroy :: FinalizerPtr RdKafkaMetadataT

rdKafkaMetadata :: RdKafkaTPtr -> Bool -> Maybe RdKafkaTopicTPtr -> Int -> IO (Either RdKafkaRespErrT RdKafkaMetadataTPtr)
rdKafkaMetadata k allTopics mt timeout = do
    tptr <- maybe (newForeignPtr_ nullPtr) pure mt
    (err, res) <- rdKafkaMetadata' k allTopics tptr timeout
    case err of
        RdKafkaRespErrNoError -> Right <$> newForeignPtr rdKafkaMetadataDestroy res
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

newUnmanagedRdKafkaTopicT :: RdKafkaTPtr -> String -> Maybe RdKafkaTopicConfTPtr -> IO (Either String RdKafkaTopicTPtr)
newUnmanagedRdKafkaTopicT kafkaPtr topic topicConfPtr = do
    duper <- maybe (newForeignPtr_ nullPtr) rdKafkaTopicConfDup topicConfPtr
    ret <- rdKafkaTopicNew kafkaPtr topic duper
    withForeignPtr ret $ \realPtr ->
        if realPtr == nullPtr then kafkaErrnoString >>= return . Left
        else return $ Right ret

newRdKafkaTopicT :: RdKafkaTPtr -> String -> Maybe RdKafkaTopicConfTPtr -> IO (Either String RdKafkaTopicTPtr)
newRdKafkaTopicT kafkaPtr topic topicConfPtr = do
    res <- newUnmanagedRdKafkaTopicT kafkaPtr topic topicConfPtr
    _ <- traverse (addForeignPtrFinalizer rdKafkaTopicDestroy') res
    return res

----------------------------------------- ADMIN API -----------------------------------------------
{#enum rd_kafka_admin_op_t as ^ {underscoreToCase} deriving (Show, Eq) #}

data RdKafkaAdminOptionsT
{#pointer *rd_kafka_AdminOptions_t as RdKafkaAdminOptionsTPtr foreign -> RdKafkaAdminOptionsT #}

{#fun rd_kafka_AdminOptions_new as ^
    {`RdKafkaTPtr', enumToCInt `RdKafkaAdminOpT'} -> `RdKafkaAdminOptionsTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_AdminOptions_destroy"
    rdKafkaAdminOptionsDestroy' :: FinalizerPtr RdKafkaAdminOptionsT

newRdKafkaAdminOptions :: RdKafkaTPtr -> RdKafkaAdminOpT -> IO RdKafkaAdminOptionsTPtr
newRdKafkaAdminOptions kafkaPtr op = do
    ret <- rdKafkaAdminOptionsNew kafkaPtr op
    addForeignPtrFinalizer rdKafkaAdminOptionsDestroy' ret
    pure ret

-- Common admin stuff
data RdKafkaTopicResultT
{#pointer *rd_kafka_topic_result_t as RdKafkaTopicResultTPtr foreign -> RdKafkaTopicResultT#}

-- Create topics

data RdKafkaNewTopicT
{#pointer *rd_kafka_NewTopic_t as RdKafkaNewTopicTPtr foreign -> RdKafkaNewTopicT #}

data RdKafkaCreateTopicsResultT
{#pointer *rd_kafka_CreateTopics_result_t as RdKafkaCreateTopicsResultTPtr foreign -> RdKafkaCreateTopicsResultT #}

{#fun rd_kafka_NewTopic_new as ^
    {`String', `Int', `Int', id `Ptr CChar', cIntConv `CSize'} -> `RdKafkaNewTopicTPtr' #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_topic_destroy"
    rdKafkaNewTopicDestroy :: FinalizerPtr RdKafkaNewTopicT

rdKafkaNewTopicDestroyArray :: [RdKafkaNewTopicTPtr] -> IO ()
rdKafkaNewTopicDestroyArray ts = do
  withForeignPtrsArrayLen ts $ \llen ptrs ->
    {#call rd_kafka_NewTopic_destroy_array#} ptrs (fromIntegral llen)

newRdKafkaNewTopic :: Text -> Int -> Int -> IO (Either Text RdKafkaNewTopicTPtr)
newRdKafkaNewTopic name partitions repFactor = do
    allocaBytes nErrorBytes $ \strPtr -> do
        ret <- rdKafkaNewTopicNew (Text.unpack name) partitions repFactor strPtr (fromIntegral nErrorBytes)
        withForeignPtr ret $ \realPtr -> do
            if realPtr == nullPtr
                then peekCText strPtr >>= pure . Left
                else addForeignPtrFinalizer rdKafkaNewTopicDestroy ret >> pure (Right ret)

newRdKafkaNewTopicUnsafe :: Text -> Int -> Int -> IO (Either Text RdKafkaNewTopicTPtr)
newRdKafkaNewTopicUnsafe name partitions repFactor = do
    allocaBytes nErrorBytes $ \strPtr -> do
        ret <- rdKafkaNewTopicNew (Text.unpack name) partitions repFactor strPtr (fromIntegral nErrorBytes)
        withForeignPtr ret $ \realPtr -> do
            if realPtr == nullPtr
                then peekCText strPtr >>= pure . Left
                else pure (Right ret)

{#fun rd_kafka_NewTopic_set_config as ^
  {`RdKafkaNewTopicTPtr', `String', `String'} -> `Either RdKafkaRespErrT ()' cIntToRespEither #}

rdKafkaCreateTopics :: RdKafkaTPtr
                    -> [RdKafkaNewTopicTPtr]
                    -> RdKafkaAdminOptionsTPtr
                    -> RdKafkaQueueTPtr
                    -> IO ()
rdKafkaCreateTopics kafkaPtr topics opts queue =
  withForeignPtr3 kafkaPtr opts queue $ \kPtr oPtr qPtr ->
    withForeignPtrsArrayLen topics $ \tLen tPtr ->
      {#call rd_kafka_CreateTopics#} kPtr tPtr (fromIntegral tLen) oPtr qPtr

rdKafkaEventCreateTopicsResult :: RdKafkaEventTPtr -> IO (Maybe RdKafkaCreateTopicsResultTPtr)
rdKafkaEventCreateTopicsResult evtPtr =
  withForeignPtr evtPtr $ \evtPtr' -> do
    res <- {#call rd_kafka_event_CreateTopics_result#} (castPtr evtPtr')
    if (res == nullPtr)
      then pure Nothing
      else Just <$> newForeignPtr_ (castPtr res)

rdKafkaCreateTopicsResultTopics :: RdKafkaCreateTopicsResultTPtr
                                -> IO [Either (String, RdKafkaRespErrT, String) String]
rdKafkaCreateTopicsResultTopics tRes =
  withForeignPtr tRes $ \tRes' ->
    alloca $ \sPtr -> do
      res <- {#call rd_kafka_CreateTopics_result_topics#} (castPtr tRes') sPtr
      size <- peekIntConv sPtr
      arr <- peekArray size res
      traverse unpackRdKafkaTopicResult arr

-- Delete topics

data RdKafkaDeleteTopicT
{#pointer *rd_kafka_DeleteTopic_t as RdKafkaDeleteTopicTPtr foreign -> RdKafkaDeleteTopicT #}

data RdKafkaDeleteTopicsResultT
{#pointer *rd_kafka_DeleteTopics_result_t as RdKafkaDeleteTopicsResultTPtr foreign -> RdKafkaDeleteTopicsResultT #}

foreign import ccall unsafe "rdkafka.h &rd_kafka_DeleteTopic_destroy"
    rdKafkaDeleteTopicDestroy :: FinalizerPtr RdKafkaDeleteTopicT

newRdKafkaDeleteTopic :: String -> IO RdKafkaDeleteTopicTPtr
newRdKafkaDeleteTopic str =
  withCString str $ \strPtr -> do
    res <- {#call rd_kafka_DeleteTopic_new#} strPtr
    newForeignPtr rdKafkaDeleteTopicDestroy res

rdKafkaDeleteTopics :: RdKafkaTPtr
                    -> [RdKafkaDeleteTopicTPtr]
                    -> RdKafkaAdminOptionsTPtr
                    -> RdKafkaQueueTPtr
                    -> IO ()
rdKafkaDeleteTopics kafkaPtr topics opts queue =
  withForeignPtr3 kafkaPtr opts queue $ \kPtr oPtr qPtr ->
    withForeignPtrsArrayLen topics $ \tLen tPtr ->
      {#call rd_kafka_DeleteTopics#} kPtr tPtr (fromIntegral tLen) oPtr qPtr

rdKafkaEventDeleteTopicsResult :: RdKafkaEventTPtr -> IO (Maybe RdKafkaDeleteTopicsResultTPtr)
rdKafkaEventDeleteTopicsResult evtPtr =
  withForeignPtr evtPtr $ \evtPtr' -> do
    res <- {#call rd_kafka_event_DeleteTopics_result#} (castPtr evtPtr')
    if (res == nullPtr)
      then pure Nothing
      else Just <$> newForeignPtr_ (castPtr res)

rdKafkaDeleteTopicsResultTopics :: RdKafkaDeleteTopicsResultTPtr
                                -> IO [Either (String, RdKafkaRespErrT, String) String]
rdKafkaDeleteTopicsResultTopics tRes =
  withForeignPtr tRes $ \tRes' ->
    alloca $ \sPtr -> do
      res <- {#call rd_kafka_DeleteTopics_result_topics#} (castPtr tRes') sPtr
      size <- peekIntConv sPtr
      arr <- peekArray size res
      traverse unpackRdKafkaTopicResult arr


-- | Unpacks raw result into
-- 'Either (topicName, errorType, errorMsg) topicName'
unpackRdKafkaTopicResult :: Ptr RdKafkaTopicResultT
                         -> IO (Either (String, RdKafkaRespErrT, String) String)
unpackRdKafkaTopicResult resPtr = do
  name <- {#call rd_kafka_topic_result_name#} resPtr >>= peekCString
  err <- {#call rd_kafka_topic_result_error#} resPtr
  case cIntToEnum err of
    RdKafkaRespErrNoError -> pure $ Right name
    respErr -> do
      errMsg <- {#call rd_kafka_topic_result_error_string#} resPtr >>= peekCString
      pure $ Left (name, respErr, errMsg)


---------------------------------------------------------------------------------------------------
-- Marshall / Unmarshall

respErrToMaybe :: RdKafkaRespErrT -> Maybe RdKafkaRespErrT
respErrToMaybe err =
  case err of
    RdKafkaRespErrNoError -> Nothing
    respErr -> Just respErr
{-# INLINE respErrToMaybe #-}

cIntToRespErr :: CInt -> Maybe RdKafkaRespErrT
cIntToRespErr = respErrToMaybe . cIntToEnum
{-# INLINE cIntToRespErr #-}

cIntToRespEither :: CInt -> Either RdKafkaRespErrT ()
cIntToRespEither err =
  case cIntToEnum err of
    RdKafkaRespErrNoError -> Right ()
    respErr -> Left respErr
{-# INLINE cIntToRespEither #-}

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

peekIntConv :: (Storable a, Integral a, Integral b) => Ptr a -> IO b
peekIntConv = liftM fromIntegral . peek

peekInt64Conv :: (Storable a, Integral a) =>  Ptr a -> IO Int64
peekInt64Conv  = liftM cIntConv . peek
{-# INLINE peekInt64Conv #-}

peekPtr :: Ptr a -> IO (Ptr b)
peekPtr = peek . castPtr
{-# INLINE peekPtr #-}

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

withForeignPtr2 :: ForeignPtr a
                -> ForeignPtr b
                -> (Ptr a -> Ptr b -> IO c)
                -> IO c
withForeignPtr2 a b f =
  withForeignPtr a $ \a' ->
    withForeignPtr b $ \b' -> f a' b'
{-# INLINE withForeignPtr2 #-}

withForeignPtr3 :: ForeignPtr a
                -> ForeignPtr b
                -> ForeignPtr c
                -> (Ptr a -> Ptr b -> Ptr c -> IO d)
                -> IO d
withForeignPtr3 a b c f =
  withForeignPtr a $ \a' ->
    withForeignPtr b $ \b' ->
      withForeignPtr c $ \c' -> f a' b' c'
{-# INLINE withForeignPtr3 #-}

withForeignPtrsArrayLen :: [ForeignPtr a] -> (Int -> Ptr (Ptr a) -> IO b) -> IO b
withForeignPtrsArrayLen as f =
  withForeignPtrs as $ \ptrs ->
    withArrayLen ptrs $ \llen pptrs -> f llen pptrs

withForeignPtrs :: [ForeignPtr a] -> ([Ptr a] -> IO b) -> IO b
withForeignPtrs as f =
  go [] as
  where
    go acc [] = f acc
    go acc (x:xs) = withForeignPtr x $ \x' -> go (x':acc) xs
