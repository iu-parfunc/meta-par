module Control.Monad.Par.Meta.Dist 
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
  , module Control.Monad.Par.Meta
) where

import Control.Monad.Par.Meta
import Control.Monad.Par.Meta.Resources.Debugging (dbgTaggedMsg)
import qualified Control.Monad.Par.Meta.Resources.Remote as Rem
import qualified Control.Monad.Par.Meta.Resources.Backoff as Bkoff
import qualified Control.Monad.Par.Meta.Resources.SingleThreaded as Single
import qualified Data.ByteString.Char8 as BS
import System.Environment (getEnvironment)
import Data.Char (ord)
import Data.List (lookup)
import Data.Monoid (mconcat, (<>))
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

--------------------------------------------------------------------------------
-- Init and Steal actions:

masterInitAction metadata trans = Single.initAction <> IA ia
  where
    ia sa scheds = do
        runIA (Rem.initAction metadata trans Rem.Master) sa scheds

slaveInitAction metadata trans =
  mconcat [ Single.initAction
          , Rem.initAction metadata trans Rem.Slave 
          , Bkoff.initAction
          ]

sa :: StealAction
sa = mconcat [ Single.stealAction 
             , Rem.stealAction    
               -- Start actually sleeping at 1ms and go up to 100ms:
             , Bkoff.mkStealAction 1000 (100*1000)
               -- Testing: A CONSTANT backoff:
--             , Bkoff.mkStealAction 1 1 
             ]

--------------------------------------------------------------------------------
-- Running and shutting down the distributed Par monad:

-- The default Transport is TCP:
runParDist mt = runParDistWithTransport mt MPI

runParDistWithTransport metadata trans comp = 
   do dbgTaggedMsg 1$ BS.pack$ "Initializing distributed Par monad with transport: "++ show trans
      Control.Exception.catch main hndlr 
 where 
   main = runMetaParIO (masterInitAction metadata (pickTrans trans)) sa comp
   hndlr e = do	BS.hPutStrLn stderr $ BS.pack $ "Exception inside runParDist: "++show e
		throw (e::SomeException)


-- Could have this for when global initialization has already happened:
-- runParDistNested = runMetaParIO (ia Nothing) sa

runParSlave meta = runParSlaveWithTransport meta MPI

runParSlaveWithTransport metadata trans = do
  dbgTaggedMsg 2 (BS.pack "runParSlave invoked.")

  -- We run a par computation that will not terminate to get the
  -- system up, running, and work-stealing:
  runMetaParIO (slaveInitAction metadata (pickTrans trans))
	       (SA $ \ x y -> do res <- runSA sa x y; 
--		                 threadDelay (10 * 1000);
	                         return res)
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
