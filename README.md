# kafka-client  
[![Circle CI](https://circleci.com/gh/haskell-works/kafka-client.svg?style=svg&circle-token=5f3ada2650dd600bc0fd4787143024867b2afc4e)](https://circleci.com/gh/haskell-works/kafka-client)

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
A working consumer example can be found here: [ConsumerExample.hs](example/ConsumerExample.hs)

```Haskell
import Data.Monoid ((<>))
import Kafka
import Kafka.Consumer

-- Global consumer properties
consumerProps :: ConsumerProperties
consumerProps = consumerBrokersList [BrokerAddress "localhost:9092"]
             <> groupId (ConsumerGroupId "consumer_example_group")
             <> noAutoCommit
             <> consumerDebug [DebugAll]

-- Subscription to topics
consumerSub :: Subscription
consumerSub = topics [TopicName "kafka-client-example-topic"]
           <> offsetReset Earliest

-- Running an example
runConsumerExample :: IO ()
runConsumerExample = do
    res <- runConsumer consumerProps consumerSub processMessages
    print $ show res

-------------------------------------------------------------------
processMessages :: KafkaConsumer -> IO (Either KafkaError ())
processMessages kafka = do
    mapM_ (\_ -> do
                   msg1 <- pollMessage kafka (Timeout 1000)
                   print $ "Message: " <> show msg1
                   err <- commitAllOffsets kafka OffsetCommit
                   print $ "Offsets: " <> maybe "Committed." show err
          ) [0 .. 10]
    return $ Right ()
```

# Producer
`kafka-client` producer supports sending messages to multiple topics.
Target topic name is a part of each message that is to be sent by `produceMessage`.

A working producer example can be found here: [ProducerExample.hs](example/ProducerExample.hs)

### Example

```Haskell
import Control.Monad (forM_)
import Kafka
import Kafka.Producer

-- Global producer properties
producerProps :: ProducerProperties
producerProps = producerBrokersList [BrokerAddress "localhost:9092"]

-- Topic to send messages to
targetTopic :: TopicName
targetTopic = TopicName "kafka-client-example-topic"

-- Run an example
runProducerExample :: IO ()
runProducerExample = do
    res <- runProducer producerProps sendMessages
    print $ show res

sendMessages :: KafkaProducer -> IO (Either KafkaError String)
sendMessages prod = do
  err1 <- produceMessage prod ProducerRecord
                                { prTopic = targetTopic
                                , prPartition = UnassignedPartition
                                , prKey = Nothing
                                , prValue = Just "test from producer"
                                }
  forM_ err1 print

  err2 <- produceMessage prod ProducerRecord
                                { prTopic = targetTopic
                                , prPartition = UnassignedPartition
                                , prKey = Just "key"
                                , prValue = Just "test from producer (with key)"
                                }
  forM_ err2 print

  return $ Right "All done."
```

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

    LDFLAGS=-L/usr/local/opt/openssl/lib CPPFLAGS=-I/usr/local/opt/openssl/include ./configure

## Installing Kafka

The full Kafka guide is at http://kafka.apache.org/documentation.html#quickstart

Alternatively `docker-compose` can be used to run Kafka locally inside a Docker container.
To run Kafka inside Docker:

```
$ docker-compose up
```
