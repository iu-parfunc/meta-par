

module DistDefaultMain (defaultMain) where


import Control.Monad.Par.Meta.Dist (longSpawn, Par, get, shutdownDist, WhichTransport(MPI),
				    runParDistWithTransport, runParSlaveWithTransport, initMPI)
import System.Environment

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
		       ans <- runParDistWithTransport metadat MPI (parcomp args)
		       putStrLn $ "Final answer: " ++ show ans
		       putStrLn $ "Calling SHUTDOWN..."
                       shutdownDist
		       putStrLn $ "... returned from shutdown, apparently successful."
        str -> error$"Unhandled mode: " ++ str
