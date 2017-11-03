module Kafka.Internal.CancellationToken
where

import Data.IORef

data CancellationStatus = Cancelled | Running deriving (Show, Eq)
newtype CancellationToken = CancellationToken (IORef CancellationStatus)

newCancellationToken :: IO CancellationToken
newCancellationToken = CancellationToken <$> newIORef Running

status :: CancellationToken -> IO CancellationStatus
status (CancellationToken ref) = readIORef ref

cancel :: CancellationToken -> IO ()
cancel (CancellationToken ref) = atomicWriteIORef ref Cancelled

