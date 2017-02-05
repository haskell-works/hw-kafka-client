module Kafka
( module X
) where

import Kafka.Consumer as X
import Kafka.Producer as X




-- -- | Sets library log level (noisiness) with respect to a kafka instance
-- setLogLevel :: Kafka -> KafkaLogLevel -> IO ()
-- setLogLevel (Kafka kptr _) level =
--   rdKafkaSetLogLevel kptr (fromEnum level)
