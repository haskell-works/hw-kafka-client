{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Producer.Types

where

import qualified Data.ByteString as BS
import           Data.Typeable

-- | Represents messages /to be enqueued/ onto a Kafka broker (i.e. used for a producer)
data ProducerRecord =
    -- | A message without a key, assigned to 'SpecifiedPartition' or 'UnassignedPartition'
    ProducerRecord
                     !ProducePartition
      {-# UNPACK #-} !BS.ByteString -- message payload

    -- | A message with a key, assigned to a partition based on the key
  | KeyedProducerRecord
      {-# UNPACK #-} !BS.ByteString -- message key
                     !ProducePartition
      {-# UNPACK #-} !BS.ByteString -- message payload
  deriving (Eq, Show, Typeable)

-- | Options for destination partition when enqueuing a message
data ProducePartition =
  -- | A specific partition in the topic
    SpecifiedPartition {-# UNPACK #-} !Int  -- the partition number of the topic

  -- | A random partition within the topic
  | UnassignedPartition
  deriving (Show, Eq, Ord, Typeable)

pmKey :: ProducerRecord -> Maybe BS.ByteString
pmKey (ProducerRecord _ _) = Nothing
pmKey (KeyedProducerRecord k _ _) = Just k

pmPartition :: ProducerRecord -> ProducePartition
pmPartition (ProducerRecord p _) = p
pmPartition (KeyedProducerRecord _ p _) = p
