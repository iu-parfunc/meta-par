{-# LANGUAGE CPP #-}

#ifndef DIST_SMP
module Control.Monad.Par.Meta.Dist 
#endif
(
    runParDist
  , runParSlave
  , runParDistWithTransport
  , runParSlaveWithTransport
  , shutdownDist
  , initMPI
  , Rem.longSpawn
  , Rem.globalRPCMetadata
  , WhichTransport(..)
  , readTransport
  , module Control.Monad.Par.Meta
) where

import Control.Monad.Par.Meta
import Control.Monad.Par.Meta.Resources.Debugging (dbgTaggedMsg)
import qualified Control.Monad.Par.Meta.Resources.Remote as Rem
import qualified Control.Monad.Par.Meta.Resources.Backoff as Bkoff

#ifdef DIST_SMP
import qualified Control.Monad.Par.Meta.Resources.SharedMemory   as Local
#else
import qualified Control.Monad.Par.Meta.Resources.SingleThreaded as Local
#endif

import qualified Data.ByteString.Char8 as BS
import System.Environment (getEnvironment)
import Data.Char (ord)
import Data.List (lookup)
import Data.Monoid (mconcat)
import Control.Monad (liftM, unless)
import Control.Monad.Par.Meta.HotVar.IORef
import Control.Exception (catch, throw, SomeException)

import Control.Parallel.MPI.Simple (commRank, commSize, commWorld)
import qualified Control.Parallel.MPI.Simple as MP

import System.Random (randomIO)
import System.IO (stderr)
import System.Posix.Process (getProcessID)
import Remote2.Reg (registerCalls)
import GHC.Conc

--------------------------------------------------------------------------------

-- | Select from available transports or provide your own.  A custom
--   implementation is required to create a transport for each node
--   given only an indication of master or slave.  Notably, it is told
--   nothing about WHICH slave is being instantiated and must
--   determine that on its own.
data WhichTransport = MPI 

instance Show WhichTransport where
  show MPI = "MPI"

readTransport :: String -> WhichTransport
readTransport "MPI"   = MPI

--------------------------------------------------------------------------------
-- Init and Steal actions:

masterResource metadata trans = 
  mconcat [ Local.mkResource
#ifdef DIST_SMP
              20
#endif
          , Rem.mkMasterResource metadata trans
          , Bkoff.mkResource 1 1
--          , Bkoff.mkResource 1000 (100*1000)
          ]

slaveResource metadata trans =
  mconcat [ Local.mkResource
#ifdef DIST_SMP
              20
#endif
          , Rem.mkSlaveResource metadata trans
          , Bkoff.mkResource 1 1
--          , Bkoff.mkResource 1000 (100*1000)
          ]

--------------------------------------------------------------------------------
-- Running and shutting down the distributed Par monad:

-- The default Transport is TCP:
runParDist mt = runParDistWithTransport mt MPI

runParDistWithTransport metadata trans comp = 
   do dbgTaggedMsg 1$ BS.pack$ "Initializing distributed Par monad with transport: "++ show trans
      Control.Exception.catch main hndlr 
 where 
   main = runMetaParIO (masterResource metadata (pickTrans trans)) comp
   hndlr e = do	BS.hPutStrLn stderr $ BS.pack $ "Exception inside runParDist: "++show e
		throw (e::SomeException)


-- Could have this for when global initialization has already happened:
-- runParDistNested = runMetaParIO (ia Nothing) sa

runParSlave meta = runParSlaveWithTransport meta MPI

runParSlaveWithTransport metadata trans = do
  dbgTaggedMsg 2 (BS.pack "runParSlave invoked.")

  -- We run a par computation that will not terminate to get the
  -- system up, running, and work-stealing:
  runMetaParIO (slaveResource metadata (pickTrans trans))
	       (new >>= get)

  fail "RETURNED FROM runMetaParIO - THIS SHOULD NOT HAPPEN"

-- This is a blocking operation that waits until shutdown is complete.
shutdownDist :: IO ()
shutdownDist = do 
   uniqueTok <- randomIO
   Rem.initiateShutdown uniqueTok
   Rem.waitForShutdown  uniqueTok
   MP.finalize

--------------------------------------------------------------------------------
-- Transport-related inititialization:
--------------------------------------------------------------------------------

pickTrans trans = 
  case trans of 
    MPI   -> \_ -> do
      rank <- commRank commWorld
      size <- commSize commWorld
      return (rank, size)

initMPI :: IO String
initMPI = do
  MP.initThread MP.Serialized
  rank <- commRank commWorld
  case rank of
    0 -> return "master"
    _ -> return "slave"
