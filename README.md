# Haskakafka 

Kafka bindings for Haskell backed by the 
[librdkafka C module](https://github.com/edenhill/librdkafka).

# High Level Consumers
High level consumers are supported by librdkafka starting from version 0.9.  
High-level consumers is their abilities to handle more than one partition and even more than one topic. 
Scalability and rebalancing are taken care of by librdkafka: once a new consumer in the same 
consumer group is started the rebalance happens and all consumer share the load.

This version of Haskakafka adds (experimental) support for high-level consumers, 
here is how such a consumer can be used in code:

```Haskell
import           Haskakafka
import           Haskakafka.Consumer

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

# Configuration Options
Configuration options are set in the call to `newKafkaConsumerConf`. For
the full list of supported options, see 
[librdkafka's list](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

# Installation

## Installing librdkafka

Although librdkafka is available on many platforms, most of
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

## Installing `kafka-client`

Since `kafka-client` uses `c2hs` to generate C bindings, you may need to 
explicitly install `c2hs` somewhere on your path (i.e. outside of a sandbox).
To do so, run:
    
    cabal install c2hs

Afterwards installation should work, so go for

    cabal install haskakafka

This uses the latest version of `kafka-client` from [Hackage](http://hackage.haskell.org/package/kafka-client)
