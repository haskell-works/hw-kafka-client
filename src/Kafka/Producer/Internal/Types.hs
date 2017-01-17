{-# LANGUAGE DeriveDataTypeable #-}
module Kafka.Producer.Internal.Types

where

import qualified Data.ByteString as BS
import           Data.Typeable

-- | Represents messages /to be enqueued/ onto a Kafka broker (i.e. used for a producer)
data ProduceMessage =
    -- | A message without a key, assigned to 'SpecifiedPartition' or 'UnassignedPartition'
    ProduceMessage
                     !ProducePartition
      {-# UNPACK #-} !BS.ByteString -- message payload

    -- | A message with a key, assigned to a partition based on the key
  | ProduceKeyedMessage
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

pmKey :: ProduceMessage -> Maybe BS.ByteString
pmKey (ProduceMessage _ _) = Nothing
pmKey (ProduceKeyedMessage k _ _) = Just k

pmPartition :: ProduceMessage -> ProducePartition
pmPartition (ProduceMessage p _) = p
pmPartition (ProduceKeyedMessage _ p _) = p
