module Kafka.Types
where
--

-- | Topic name to be consumed
--
-- Wildcard (regex) topics are supported by the librdkafka assignor:
-- any topic name in the topics list that is prefixed with @^@ will
-- be regex-matched to the full list of topics in the cluster and matching
-- topics will be added to the subscription list.
newtype TopicName =
    TopicName String -- ^ a simple topic name or a regex if started with @^@
    deriving (Show, Eq, Read)

-- | Kafka broker address string (e.g. @broker1:9092@)
newtype BrokerAddress = BrokerAddress String deriving (Show, Eq)

-- | Timeout in milliseconds
newtype Timeout = Timeout Int deriving (Show, Eq, Read)
