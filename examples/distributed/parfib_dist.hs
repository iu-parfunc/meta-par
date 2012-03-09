{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -O2 -ddump-splices #-}
import Data.Int (Int64)
import System.Environment (getArgs)
import Control.Monad.IO.Class (liftIO)
-- import Control.Monad.Par.Meta.Dist 
import Control.Concurrent   (myThreadId)
import System.Process       (readProcess)
import System.Posix.Process (getProcessID)
import Data.Char            (isSpace)

import Control.Monad.Par.Meta.DistSMP
        (longSpawn, Par, get, shutdownDist, WhichTransport(MPI),
	 runParDistWithTransport, runParSlaveWithTransport, spawn_, initMPI)
-- Tweaked version of CloudHaskell's closures:
import Remote2.Call (mkClosureRec, remotable)
import DistDefaultMain

type FibType = Int64

--------------------------------------------------------------------------------

-- Par monad version + distributed execution:
-- This version is NOT thresholded.
parfib0 :: Int -> Par FibType
parfib0 n | n < 2     = return 1
	  | otherwise = do
    xf <- longSpawn$ $(mkClosureRec 'parfib0) (n-1)
    y  <-             parfib0                 (n-2)
    x  <- get xf
    return (x+y)

--------------------------------------------------------------------------------

-- Par monad version + distributed execution:
parfib1 :: (Int,Int) -> Par FibType
parfib1 (n,thresh) | n < 2       = return 1
		   | n <= thresh = parfib2 n
		   | otherwise   = do
#if 0
    liftIO $ do 
       mypid <- getProcessID
       mytid <- myThreadId
       host  <- hostName
       putStrLn $ " [host "++host++" pid "++show mypid++" "++show mytid++"] PARFIB "++show n
#endif
    xf <- longSpawn$ $(mkClosureRec 'parfib1) (n-1, thresh)
    y  <-             parfib1                 (n-2, thresh)
    x  <- get xf
    return (x+y)

parfib2 :: Int -> Par FibType
parfib2 n | n < 2 = return 1
parfib2 n = do 
    xf <- spawn_$ parfib2 (n-1)
    y  <-         parfib2 (n-2)
    x  <- get xf
    return (x+y)

------------------------------------------------------------

hostName = do s <- readProcess "hostname" [] ""
	      return (trim s)
 where 
  -- | Trim whitespace from both ends of a string.
  trim :: String -> String
  trim = f . f
     where f = reverse . dropWhile isSpace


-- Generate stub code for RPC:
remotable ['parfib1, 'parfib0]

main = do 
    args <- getArgs
    let (size, cutoff) = case args of 
            []        -> (10, 1)
            [n]   -> (read n, 1)
            [n,c] -> (read n, read c)

    role <- initMPI
    case role of 
        "slave" -> runParSlaveWithTransport [__remoteCallMetaData] MPI
        "master" -> do 
                       putStr$ "Running parfib with settings: "
                       putStrLn$ show ("mpi", size, cutoff)

		       putStrLn "Using non-thresholded version:"
		       ans <- (runParDistWithTransport [__remoteCallMetaData] MPI
			       (parfib1 (size,cutoff)) :: IO FibType)
                       putStrLn $ "Final answer: " ++ show ans
                       putStrLn $ "Calling SHUTDOWN..."
                       shutdownDist
                       putStrLn $ "... returned from shutdown, apparently successful."

        str -> error$"Unhandled mode: " ++ str
