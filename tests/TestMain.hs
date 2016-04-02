{-# LANGUAGE ScopedTypeVariables #-}
module Main (main) where
import           Kafka

import           Control.Exception
import           Control.Monad
import           System.Environment
import           Test.Hspec
import           Text.Regex.Posix

import qualified Data.ByteString.Char8 as C8
import qualified Data.Map              as Map

brokerAddress :: IO String
brokerAddress = getEnv "KAFKA_TEST_BROKER" `catch` \(_ :: SomeException) -> (return "localhost:9092")
brokerTopic :: IO String
brokerTopic = getEnv "KAFKA_TEST_TOPIC" `catch` \(_ :: SomeException) -> (return "kafka-client_tests")
kafkaDelay :: Int -- Little delay for operation
kafkaDelay = 5 *  1000

getAddressTopic :: (String -> String -> IO ()) -> IO ()
getAddressTopic cb = do
  b <- brokerAddress
  t <- brokerTopic
  cb b t

sampleProduceMessages :: [KafkaProduceMessage]
sampleProduceMessages =
  [ KafkaProduceMessage $ C8.pack "hello"
  , KafkaProduceKeyedMessage (C8.pack "key") (C8.pack "value")
  , KafkaProduceMessage $ C8.pack "goodbye"
  ]

shouldBeProduceConsume :: KafkaProduceMessage -> ReceivedMessage -> IO ()
shouldBeProduceConsume (KafkaProduceMessage ppayload) m = do
  messagePayload m `shouldBe` ppayload
  messageKey m `shouldBe` Nothing

shouldBeProduceConsume (KafkaProduceKeyedMessage pkey ppayload) m = do
  ppayload `shouldBe` messagePayload m
  Just pkey `shouldBe` messageKey m

primeEOF :: KafkaTopic -> IO ()
primeEOF kt = void (consumeMessage kt 0 1000)

testmain :: IO ()
testmain = hspec $ do
  describe "RdKafka versioning" $
    it "should be a valid version number" $
      rdKafkaVersionStr `shouldSatisfy` (=~"[0-9]+(.[0-9]+)+")

  describe "Kafka Configuration" $ do
    it "should allow dumping" $ do
      kConf <- newKafkaConf
      kvs <- dumpKafkaConf kConf
      Map.size kvs `shouldSatisfy` (> 0)

    it "should change when set is called" $ do
      kConf <- newKafkaConf
      setKafkaConfValue kConf "socket.timeout.ms" "50000"
      kvs <- dumpKafkaConf kConf
      (kvs Map.! "socket.timeout.ms") `shouldBe` "50000"

    it "should throw an exception on unknown property" $ do
      kConf <- newKafkaConf
      setKafkaConfValue kConf "blippity.blop.cosby" "120" `shouldThrow`
        (\(KafkaUnknownConfigurationKey str) -> not $ null str)

    it "should throw an exception on an invalid value" $ do
      kConf <- newKafkaConf
      setKafkaConfValue kConf "socket.timeout.ms" "monorail" `shouldThrow`
        (\(KafkaInvalidConfigurationValue str) -> not $ null str)

  describe "Kafka topic configuration" $ do
    it "should allow dumping" $ do
      kConf <- newTopicConf
      kvs <- dumpTopicConf kConf
      Map.size kvs `shouldSatisfy` (> 0)

    it "should change when set is called" $ do
      kConf <- newTopicConf
      setTopicConfValue kConf "request.timeout.ms" "20000"
      kvs <- dumpTopicConf kConf
      (kvs Map.! "request.timeout.ms") `shouldBe` "20000"

    it "should throw an exception on unknown property" $ do
      kConf <- newTopicConf
      setTopicConfValue kConf "blippity.blop.cosby" "120" `shouldThrow`
        (\(KafkaUnknownConfigurationKey str) -> not $ null str)

    it "should throw an exception on an invalid value" $ do
      kConf <- newTopicConf
      setTopicConfValue kConf "request.timeout.ms" "mono...doh!" `shouldThrow`
        (\(KafkaInvalidConfigurationValue str) -> not $ null str)

  describe "Logging" $ do
    it "should allow setting of log level" $ getAddressTopic $ \a t ->
      withKafkaConsumer [] [] a t 0 PartitionOffsetEnd $ \kafka _ ->
        setLogLevel kafka KafkaLogDebug

  describe "Consume and produce cycle" $ do
    it "should be able to produce and consume a unkeyed message off of the broker" $ getAddressTopic $ \a t -> do
      let message = KafkaProduceMessage (C8.pack "hey hey we're the monkeys")
      withKafkaConsumer [] [] a t 0 PartitionOffsetEnd $ \_ topic -> do
        primeEOF topic
        perr <- withKafkaProducer [] [] a t $ \_ producerTopic ->
                produceMessage producerTopic (KafkaSpecifiedPartition 0) message
        perr `shouldBe` Nothing

        et <- consumeMessage topic 0 kafkaDelay
        case et of
          Left err -> error $ show err
          Right m -> message `shouldBeProduceConsume` m

    it "should be able to produce and consume a keyed message" $ getAddressTopic $ \a t -> do
      let message = KafkaProduceKeyedMessage (C8.pack "key") (C8.pack "monkey around")

      withKafkaConsumer [] [] a t 0 PartitionOffsetEnd $ \_ topic -> do
        primeEOF topic
        perr <- withKafkaProducer [] [] a t $ \_ producerTopic ->
                  produceKeyedMessage producerTopic message
        perr `shouldBe` Nothing

        et <- consumeMessage topic 0 kafkaDelay
        case et of
          Left err -> error $ show err
          Right m -> message `shouldBeProduceConsume` m

    it "should be able to batch produce messages" $ getAddressTopic $ \a t ->
      withKafkaConsumer [] [] a t 0 PartitionOffsetEnd $ \_ topic -> do
        primeEOF topic
        errs <- withKafkaProducer [] [] a t $ \_ producerTopic ->
                  produceMessageBatch producerTopic (KafkaSpecifiedPartition 0) sampleProduceMessages
        errs `shouldBe` []

        ets <- mapM (\_ -> consumeMessage topic 0 kafkaDelay) ([1..3] :: [Integer])

        forM_ (zip sampleProduceMessages ets) $ \(pm, et) ->
          case (pm, et) of
            (_, Left err) -> error $ show err
            (pmessage, Right cm) -> pmessage `shouldBeProduceConsume` cm


-- Test setup (error on no Kafka)
checkForKafka :: IO Bool
checkForKafka = do
  a <- brokerAddress
  me <- fetchBrokerMetadata [] a 1000
  return $ case me of
    (Left _) -> False
    (Right _) -> True

main :: IO ()
main = do
  a <- brokerAddress
  hasKafka <- checkForKafka
  if hasKafka then testmain
  else error $ "\n\n\
    \*******************************************************************************\n\
    \ Integration tests require an operable Kafka broker running on " ++ a ++ "\n\
    \ please follow the guide in Readme.md to set this up                          \n\
    \*******************************************************************************\n"
