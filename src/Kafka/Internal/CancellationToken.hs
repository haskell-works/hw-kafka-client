module Kafka.Internal.CancellationToken
( CancellationStatus(..)
, CancellationToken(..)
, newCancellationToken
, status
, cancel
)
where

import Data.IORef (IORef, newIORef, readIORef, atomicWriteIORef)

data CancellationStatus = Cancelled | Running deriving (Show, Eq)
newtype CancellationToken = CancellationToken (IORef CancellationStatus)

newCancellationToken :: IO CancellationToken
newCancellationToken = CancellationToken <$> newIORef Running

status :: CancellationToken -> IO CancellationStatus
status (CancellationToken ref) = readIORef ref

cancel :: CancellationToken -> IO ()
cancel (CancellationToken ref) = atomicWriteIORef ref Cancelled

