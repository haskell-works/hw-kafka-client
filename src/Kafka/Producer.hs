{-# LANGUAGE TupleSections #-}
module Kafka.Producer
( module X
, runProducer
, newProducer
, produceMessage, produceMessageBatch, produceMessageSync
, flushProducer
, closeProducer
, KafkaProducer
, RdKafkaRespErrT (..)
)
where

import           Control.Arrow                    ((&&&))
import           Control.Exception                (bracket)
import           Control.Monad                    (forM, forM_, (<=<))
import           Control.Monad.IO.Class           (MonadIO(liftIO))
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Internal         as BSI
import           Data.Function                    (on)
import           Data.List                        (groupBy, sortBy)
import           Data.Ord                         (comparing)
import qualified Data.Text                        as Text
import           Foreign.C.Error                  (getErrno)
import           Foreign.ForeignPtr               (withForeignPtr, newForeignPtr_)
import           Foreign.Marshal.Array            (withArrayLen)
import           Foreign.Marshal.Alloc
import           Foreign.Ptr                      (Ptr, castPtr, nullPtr, plusPtr)
import           Foreign.Storable                 (Storable(..))
import           Kafka.Internal.CancellationToken as CToken
import           Kafka.Internal.RdKafka
  ( RdKafkaMessageT(..)
  , RdKafkaTypeT(..)
  , RdKafkaRespErrT(..)
  , newRdKafkaT
  , newUnmanagedRdKafkaTopicT
  , destroyUnmanagedRdKafkaTopic
  , rdKafkaProduce
  , rdKafkaSetLogLevel
  , rdKafkaProduceBatch
  , rdKafkaOutqLen
  , rdKafkaErrno2err
  )
import           Kafka.Internal.Setup             (KafkaConf(..), Kafka(..), TopicConf(..), kafkaConf, KafkaProps(..), topicConf, TopicProps(..))
import           Kafka.Internal.Shared            (pollEvents)
import           Kafka.Producer.Convert           (copyMsgFlags, producePartitionCInt, producePartitionInt, handleProduceErr)
import           Kafka.Producer.Types             (KafkaProducer(..))

import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types              as X hiding (KafkaProducer)
import Kafka.Types                       as X
import Kafka.Internal.Shared             (kafkaRespErr)

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- or/and 'produceMessageBatch' to send messages to Kafka.
{-# DEPRECATED runProducer "Use newProducer/closeProducer instead" #-}
runProducer :: ProducerProperties
            -> (KafkaProducer -> IO (Either KafkaError a))
            -> IO (Either KafkaError a)
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer props

    clProducer (Left _)     = return ()
    clProducer (Right prod) = closeProducer prod

    runHandler (Left err)   = return $ Left err
    runHandler (Right prod) = f prod

-- | Creates a new kafka producer
-- A newly created producer must be closed with 'closeProducer' function.
newProducer :: MonadIO m => ProducerProperties -> m (Either KafkaError KafkaProducer)
newProducer pps = liftIO $ do
  kc@(KafkaConf kc' _ _) <- kafkaConf (KafkaProps $ (ppKafkaProps pps))
  tc <- topicConf (TopicProps $ (ppTopicProps pps))

  -- set callbacks
  forM_ (ppCallbacks pps) (\setCb -> setCb kc)

  mbKafka <- newRdKafkaT RdKafkaProducer kc'
  case mbKafka of
    Left err    -> return . Left $ KafkaError err
    Right kafka -> do
      forM_ (ppLogLevel pps) (rdKafkaSetLogLevel kafka . fromEnum)
      let prod = KafkaProducer (Kafka kafka) kc tc
      return (Right prod)

-- | Sends a single message.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessage :: MonadIO m
               => KafkaProducer
               -> ProducerRecord
               -> m (Maybe KafkaError)
produceMessage kp@(KafkaProducer (Kafka k) _ (TopicConf tc)) m = liftIO $ do
  pollEvents kp (Just $ Timeout 0) -- fire callbacks if any exist (handle delivery reports)
  bracket (mkTopic $ prTopic m) clTopic withTopic
    where
      mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k (Text.unpack tn) (Just tc)

      clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

      withTopic (Left err) = return . Just . KafkaError $ Text.pack err
      withTopic (Right t) =
        withBS (prValue m) $ \payloadPtr payloadLength ->
          withBS (prKey m) $ \keyPtr keyLength ->
            handleProduceErr =<<
              rdKafkaProduce t (producePartitionCInt (prPartition m))
                copyMsgFlags payloadPtr (fromIntegral payloadLength)
                keyPtr (fromIntegral keyLength) nullPtr


-- | Sends a single message synchronously
--
--   This function implements a simple synchronous production of a message by
--   passing along an error pointer. Once this pointer has been set to an error
--   the message can be considered sent. If the message is sent with
--   'RdKafkaRespErrNoError', then this returns a `Nothing`
--
--   Example C-solution used: https://github.com/edenhill/librdkafka/wiki/Sync-producer
produceMessageSync :: MonadIO m
                   => KafkaProducer
                   -> ProducerRecord
                   -> m (Either KafkaError ())
produceMessageSync (KafkaProducer (Kafka k) _ (TopicConf tc)) m = liftIO $ do
  bracket (mkTopic $ prTopic m) clTopic withTopic
    where
      mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k (Text.unpack tn) (Just tc)

      clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

      withTopic (Left err) = pure . Left . KafkaError $ Text.pack err
      withTopic (Right t) =
        withBS (prValue m) $ \payloadPtr payloadLength ->
          withBS (prKey m) $ \keyPtr keyLength ->
            withPtr $ \resPtr ->
              let
                produce =
                  rdKafkaProduce t (producePartitionCInt (prPartition m))
                    copyMsgFlags payloadPtr (fromIntegral payloadLength)
                    keyPtr (fromIntegral keyLength) resPtr
              in do
                res <- produce
                if res == (-1 :: Int) then
                  (Left . kafkaRespErr) <$> getErrno
                else
                  waitForAck resPtr

      initialError :: Int
      initialError = -12345

      withPtr :: (Ptr RdKafkaRespErrT -> IO (Either KafkaError ())) -> IO (Either KafkaError ())
      withPtr f = alloca $ \ptr -> do
        poke ptr initialError
        f $ castPtr ptr

      waitForAck :: Ptr RdKafkaRespErrT -> IO (Either KafkaError ())
      waitForAck ptr = do
        currentVal <- peek $ castPtr ptr
        if currentVal == initialError then do
          waitForAck ptr
        else
          pure $ case rdKafkaErrno2err currentVal of
            RdKafkaRespErrNoError -> Right ()
            err -> Left (KafkaResponseError err)


-- | Sends a batch of messages.
-- Returns a list of messages which it was unable to send with corresponding errors.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessageBatch :: MonadIO m
                    => KafkaProducer
                    -> [ProducerRecord]
                    -> m [(ProducerRecord, KafkaError)]
                    -- ^ An empty list when the operation is successful,
                    -- otherwise a list of "failed" messages with corresponsing errors.
produceMessageBatch kp@(KafkaProducer (Kafka k) _ (TopicConf tc)) messages = liftIO $ do
  pollEvents kp (Just $ Timeout 0) -- fire callbacks if any exist (handle delivery reports)
  concat <$> forM (mkBatches messages) sendBatch
  where
    mkSortKey = prTopic &&& prPartition
    mkBatches = groupBy ((==) `on` mkSortKey) . sortBy (comparing mkSortKey)

    mkTopic (TopicName tn) = newUnmanagedRdKafkaTopicT k (Text.unpack tn) (Just tc)

    clTopic = either (return . const ()) destroyUnmanagedRdKafkaTopic

    sendBatch []    = return []
    sendBatch batch = bracket (mkTopic $ prTopic (head batch)) clTopic (withTopic batch)

    withTopic ms (Left err) = return $ (, KafkaError (Text.pack err)) <$> ms
    withTopic ms (Right t) = do
      let (partInt, partCInt) = (producePartitionInt &&& producePartitionCInt) $ prPartition (head ms)
      withForeignPtr t $ \topicPtr -> do
        nativeMs <- forM ms (toNativeMessage topicPtr partInt)
        withArrayLen nativeMs $ \len batchPtr -> do
          batchPtrF <- newForeignPtr_ batchPtr
          numRet    <- rdKafkaProduceBatch t partCInt copyMsgFlags batchPtrF len
          if numRet == len then return []
          else do
            errs <- mapM (return . err'RdKafkaMessageT <=< peekElemOff batchPtr)
                         [0..(fromIntegral $ len - 1)]
            return [(m, KafkaResponseError e) | (m, e) <- zip messages errs, e /= RdKafkaRespErrNoError]

    toNativeMessage t p m =
      withBS (prValue m) $ \payloadPtr payloadLength ->
        withBS (prKey m) $ \keyPtr keyLength ->
          return RdKafkaMessageT
            { err'RdKafkaMessageT       = RdKafkaRespErrNoError
            , topic'RdKafkaMessageT     = t
            , partition'RdKafkaMessageT = p
            , len'RdKafkaMessageT       = payloadLength
            , payload'RdKafkaMessageT   = payloadPtr
            , offset'RdKafkaMessageT    = 0
            , keyLen'RdKafkaMessageT    = keyLength
            , key'RdKafkaMessageT       = keyPtr
            }

-- | Closes the producer.
-- Will wait until the outbound queue is drained before returning the control.
closeProducer :: MonadIO m => KafkaProducer -> m ()
closeProducer p =
  let (KafkaConf _ _ ct) = kpKafkaConf p
  in liftIO (CToken.cancel ct) >> flushProducer p

-- | Drains the outbound queue for a producer.
--  This function is also called automatically when the producer is closed
-- with 'closeProducer' to ensure that all queued messages make it to Kafka.
flushProducer :: MonadIO m => KafkaProducer -> m ()
flushProducer kp = liftIO $ do
    pollEvents kp (Just $ Timeout 100)
    l <- outboundQueueLength (kpKafkaPtr kp)
    if (l == 0)
      then pollEvents kp (Just $ Timeout 0) -- to be sure that all the delivery reports are fired
      else flushProducer kp

------------------------------------------------------------------------------------

withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka k) = rdKafkaOutqLen k
