# kafka-client 

Kafka bindings for Haskell backed by the 
[librdkafka C module](https://github.com/edenhill/librdkafka).

## Credits
This project is inspired by [Haskakafka](https://github.com/cosbynator/haskakafka) 
which unfortunately doesn't seem to be actively maintained.

# Consumer
High level consumers are supported by `librdkafka` starting from version 0.9.  
High-level consumers provide an abstraction for consuming messages from multiple 
partitions and topics. They are also address scalability (up to a number of partitions)
by providing automatic rebalancing functionality. When a new consumer joins a consumer 
group the set of consumers attempt to "rebalance" the load to assign partitions to each consumer.

### Example:

```Haskell
import Kafka.Consumer

runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer
              (ConsumerGroupId "test_group")    -- group id is required
              []                                -- extra kafka conf properties
              (BrokersString "localhost:9092")  -- kafka brokers to connect to
              [TopicName "^hl-test*"]           -- list of topics to consume, supporting regex
              processMessages                   -- handler to consume messages
    print $ show res

-- this function is used inside consumer 
-- and it is responsible for polling and handling messages
-- In this case I will do 10 polls and then return a success
processMessages :: Kafka -> IO (Either KafkaError ())
processMessages kafka = do
    mapM_ (\_ -> do
                   msg1 <- pollMessage kafka 1000
                   print $ show msg1) [1..10]
    return $ Right ()
    
```

Other examples (including using a rebalance callback) can be found here: [ConsumerExample.hs](src/Kafka/Examples/ConsumerExample.hs)

### Configuration Options
Configuration options are set in the call to `newKafkaConsumerConf`. For
the full list of supported options, see 
[librdkafka's list](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

# Producer

`kafka-client` allows sending messages to multiple topics from one producer.  
In fact, `kafka-client` does not try to manage target topics, it is up to the API user to decide 
which topics to produce to and how to manage them.

### Example

```Haskell
import Kafka.Producer

runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer 
              [] 
              (BrokersString "localhost:9092") 
              sendMessages
    print $ show res

-- This callback function just need to return an IO of anything.
sendMessages :: Kafka -> IO String
sendMessages kafka = do
    -- reference a topic (or a list of topics if needed)
    topic <- newKafkaTopic kafka "hl-test" []

    -- produce a message without a key to a random partition
    err1 <- produceMessage topic KafkaUnassignedPartition (KafkaProduceMessage "test from producer")
    print $ show err1

    -- produce a message with a key, a target partition will be determined by the key.
    err2 <- produceMessage topic KafkaUnassignedPartition (KafkaProduceKeyedMessage "key" "test from producer (with key)")
    print $ show err2

    return "All done."
```

This can be found here: [ProducerExample.hs](src/Kafka/Examples/ProducerExample.hs)

# Installation

## Installing librdkafka

Although `librdkafka` is available on many platforms, most of
the distribution packages are too old to support `kafka-client`.
As such, we suggest you install from the source:

    git clone https://github.com/edenhill/librdkafka
    cd librdkafka
    ./configure
    make && sudo make install

Sometimes it is helpful to specify openssl includes explicitly:

    LDFLAGS=-L/usr/local/opt/openssl/include CPPFLAGS=-I/usr/local/opt/openssl/include ./configure

## Installing Kafka

The full Kafka guide is at http://kafka.apache.org/documentation.html#quickstart
