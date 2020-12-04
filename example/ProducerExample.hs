{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module ProducerExample
where

import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception       (bracket)
import Control.Monad           (forM_)
import Control.Monad.IO.Class  (MonadIO(..))
import Data.ByteString         (ByteString)
import Data.ByteString.Char8   (pack)
import Kafka.Consumer          (Offset)
import Kafka.Producer
import Data.Text               (Text)

-- Global producer properties
producerProps :: ProducerProperties
producerProps = brokersList ["localhost:9092"]
             <> sendTimeout (Timeout 10000)
             <> setCallback (deliveryCallback print)
             <> logLevel KafkaLogDebug

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = "kafka-client-example-topic"

mkMessage :: Maybe ByteString -> Maybe ByteString -> ProducerRecord
mkMessage k v = ProducerRecord
                  { prTopic = targetTopic
                  , prPartition = UnassignedPartition
                  , prKey = k
                  , prValue = v
                  }

-- Run an example
runProducerExample :: IO ()
runProducerExample =
    bracket mkProducer clProducer runHandler >>= print
    where
      mkProducer = newProducer producerProps
      clProducer (Left _)     = return ()
      clProducer (Right prod) = closeProducer prod
      runHandler (Left err)   = return $ Left err
      runHandler (Right prod) = sendMessages prod

sendMessages :: KafkaProducer -> IO (Either KafkaError ())
sendMessages prod = do
  putStrLn "Producer is ready, send your messages!"
  msg1 <- getLine

  err1 <- produceMessage prod (mkMessage Nothing (Just $ pack msg1))
  forM_ err1 print

  putStrLn "One more time!"
  msg2 <- getLine

  err2 <- produceMessage prod (mkMessage (Just "key") (Just $ pack msg2))
  forM_ err2 print

  putStrLn "And the last one..."
  msg3 <- getLine
  err3 <- produceMessage prod (mkMessage (Just "key3") (Just $ pack msg3))

  -- errs <- produceMessageBatch prod
  --           [ mkMessage (Just "b-1") (Just "batch-1")
  --           , mkMessage (Just "b-2") (Just "batch-2")
  --           , mkMessage Nothing      (Just "batch-3")
  --           ]

  -- forM_ errs (print . snd)

  putStrLn "Thank you."
  return $ Right ()

-- | An example for sending messages synchronously using the 'produceMessage''
--   function
--
sendMessageSync :: MonadIO m
                => KafkaProducer
                -> ProducerRecord
                -> m (Either KafkaError Offset)
sendMessageSync producer record = liftIO $ do
  -- Create an empty MVar:
  var <- newEmptyMVar

  -- Produce the message and use the callback to put the delivery report in the
  -- MVar:
  res <- produceMessage' producer record (putMVar var)

  case res of
    Left (ImmediateError err) ->
      pure (Left err)
    Right () -> do
      -- Flush producer queue to make sure you don't get stuck waiting for the
      -- message to send:
      flushProducer producer

      -- Wait for the message's delivery report and map accordingly:
      takeMVar var >>= return . \case
        DeliverySuccess _ offset -> Right offset
        DeliveryFailure _ err    -> Left err
        NoMessageError err       -> Left err
