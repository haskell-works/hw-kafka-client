module Main where

import ConsumerExample
import ProducerExample

main :: IO ()
main = do
  putStrLn "Running producer example..."
  runProducerExample

  putStrLn "Running consumer example..."
  runConsumerExample

  putStrLn "Ok."
