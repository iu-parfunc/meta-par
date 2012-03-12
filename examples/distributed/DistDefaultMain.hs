

module DistDefaultMain (defaultMain) where


import Control.Monad.Par.Meta.DistSMP
       (shutdownDist, WhichTransport(MPI),
	runParDistWithTransport, runParSlaveWithTransport, initMPI)
import Data.Time.Clock
import System.Environment
import GHC.Conc
import Control.Concurrent.MVar
import Control.Monad.Trans (liftIO)

-- defaultMain metadat parcomp defaults = do 
--defaultMain metadat parcomp defaults parseargs = do 
defaultMain metadat parcomp numargs parseargs = do 
    args <- getArgs
--    args <- mapM (\(f,x) -> f x) (zip defaults rest)
    let 
    -- TODO: How can we get the ranks here?  Need to parse MACHINE_LIST
        ranks = 4 

--    let strs = rest ++ drop (length rest) defaults
    let strs = take numargs (args ++ repeat "")
    args <- parseargs ranks strs

    role <- initMPI
    case role of 
        "slave" -> runParSlaveWithTransport metadat MPI
        "master" -> do 
--                        putStrLn "Running dummy computation to get connected..."
-- 		       runParDistWithTransport metadat trans (return "hello")
--                        putStrLn "All done with dummy computation.  Now the real thing."

              -- Adam came up with an even better hack:
                       mv    <- newEmptyMVar
                       start <- newEmptyMVar
                       -- Block until we start up the par computation:
                       forkIO $ do takeMVar mv 
				   getCurrentTime >>= putMVar start

		       ans   <- runParDistWithTransport metadat MPI 
                                    -- First, start timing by unblocking the thread:
				    (do liftIO$ putMVar mv ()
				        parcomp args)
                       end    <- getCurrentTime
                       start' <- takeMVar start

		       putStrLn $ "Final answer: " ++ show ans
                       putStrLn $ "SELFTIMED: "++ show ((fromRational $ toRational $ diffUTCTime end start') :: Double)
		       putStrLn $ "Calling SHUTDOWN..."
                       shutdownDist
		       putStrLn $ "... returned from shutdown, apparently successful."
        str -> error$"Unhandled mode: " ++ str
