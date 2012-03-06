{-# LANGUAGE CPP, BangPatterns, PackageImports #-}
{-# LANGUAGE NamedFieldPuns, DeriveGeneric, ScopedTypeVariables, DeriveDataTypeable #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fwarn-unused-imports #-}

-- Turn this on to do extra invariant checking
#define AUDIT_WORK
-- define TMPDBG

-- | Resource for Remote execution.

module Control.Monad.Par.Meta.Resources.Remote 
  ( initAction, stealAction, 
    initiateShutdown, waitForShutdown, 
    longSpawn, 
    InitMode(..),
    globalRPCMetadata
  )  
 where

import "mtl" Control.Monad.Reader (ask)
import Control.Applicative    ((<$>))
import Control.Concurrent     (myThreadId, threadDelay, writeChan, readChan, newChan, Chan,
			       forkOS, threadCapability, ThreadId, killThread)
import Control.DeepSeq        (NFData)
import Control.Exception      (catch, SomeException)
import Control.Monad          (forM, forM_, when, unless, liftM)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Par.Meta.HotVar.IORef
import Data.Typeable          (Typeable)
import Data.IORef             (writeIORef, readIORef, newIORef, IORef)
import qualified Data.IntMap as IntMap
import qualified Data.Map    as M
import Data.Set               (Set)
import qualified Data.Set    as Set
import Data.Int               (Int64)
import Data.Word              (Word8, Word32)
import Data.Maybe             (fromMaybe)
import Data.Char              (isSpace)
import qualified Data.ByteString.Char8 as BS
import Data.Concurrent.Deque.Reference as R
import Data.Concurrent.Deque.Class     as DQ
import Data.List as L
import Data.List.Split    (splitOn)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV

import Data.Serialize (encode, decode, Serialize)
import qualified Data.Serialize as Ser
-- import Data.Serialize.Derive (deriveGet, derivePut)
import GHC.Generics       (Generic)
import System.IO          (hFlush, stdout, stderr)
import System.IO.Unsafe   (unsafePerformIO)
import System.Process     (readProcess)
import System.Directory   (removeFile, doesFileExist, renameFile)
import System.Random      (randomIO)

import Control.Monad.Par.Meta.Resources.Debugging
   (dbg, dbgTaggedMsg, dbgDelay, dbgCharMsg, taggedmsg_global_mode)
import Control.Monad.Par.Meta (forkWithExceptions, new, put_, Sched(Sched,no,ivarUID,tids),
			       IVar, Par, InitAction(IA), StealAction(SA))
import Control.Parallel.MPI.Simple (commWorld, anySource, anyTag, fromRank, toRank)
import qualified Control.Parallel.MPI.Simple     as MP
import Remote2.Closure  (Closure(Closure))
import Remote2.Encoding (Payload, Serializable, serialDecodePure, getPayloadContent, getPayloadType)
import qualified Remote2.Reg as Reg
import GHC.Conc (numCapabilities)

----------------------------------------------------------------------------------------------------
--                                              TODO                                              --
----------------------------------------------------------------------------------------------------

--   * Add configurability for constants below.
--   * Get rid of myTransport -- just use the peerTable
--   * Rewrite serialization using putWordhost

-----------------------------------------------------------------------------------
-- Data Types
-----------------------------------------------------------------------------------

-- | For now it is the master's job to know the machine list:
data InitMode = Master | Slave

type NodeID = MP.Rank

-- Format a nodeID:
showNodeID n = "<Node"+++ sho n+++">"
a +++ b = BS.append a b

sho :: Show a => a -> BS.ByteString
sho = BS.pack . show

-- Control messages are for starting the system up and communicating
-- between slaves and the master.  

-- We group all messages under one datatype, though often these
-- messages are used in disjoint phases and could be separate
-- datatypes.
data Message = 
   -- First CONTROL MESSAGES:   
   ----------------------------------------
     -- The master grants permission to start stealing.
     -- No one had better send the master steal requests before this goes out!!
   StartStealing
     
   | ShutDown !Token -- A message from master to slave, "exit immediately":
   | ShutDownACK    -- "I heard you, shutting down gracefully"

   -- Second, WORK MESSAGES:   
   ----------------------------------------
   -- Work messages are for getting things done (stealing work,
   -- returning results).
   ----------------------------------------

     -- | A message signifying a steal request and including the NodeID of
     --   the sender:
   | StealRequest !NodeID
   | StealResponse !NodeID (IVarId, Closure Payload)

     -- The result of a parallel computation, evaluated remotely:
   | WorkFinished !NodeID !IVarId Payload

  deriving (Show, Generic, Typeable)


-- | Unique (per-machine) identifier for 'IVar's
type IVarId = Int

type ParClosure a = (Par a, Closure (Par a))


-----------------------------------------------------------------------------------
-- Global constants:
-----------------------------------------------------------------------------------

-- Other temporary toggles:
-- define SHUTDOWNACK


--------------------------------------------------------------------------------
-- Global Mutable Structures 
--------------------------------------------------------------------------------

-- TODO: Several of these things (myNodeID, masterID, etc) could
-- become immutable, packaged in a config record, and be passed around
-- to the relavant places.


-- An item of work that can be executed either locally, or stolen
-- through the network.
data LongWork = LongWork {
     localver  :: Par (),
     stealver  :: Maybe (IVarId, Closure Payload)
  }

{-# NOINLINE longQueue #-}
longQueue :: DQ.Queue LongWork
longQueue = unsafePerformIO $ R.newQ

-- Each peer is either connected, or not connected yet.
type PeerName = BS.ByteString

{-# NOINLINE myNodeID #-}
myNodeID :: HotVar NodeID
myNodeID = unsafePerformIO$ newIORef (error "uninitialized global 'myNodeID'")

{-# NOINLINE masterID #-}
masterID :: HotVar NodeID
masterID = unsafePerformIO$ newIORef (error "uninitialized global 'masterID'")

{-# NOINLINE isMaster #-}
isMaster :: HotVar Bool
isMaster = unsafePerformIO$ newIORef False

{-# NOINLINE worldSize #-}
worldSize :: HotVar Int
worldSize = unsafePerformIO$ newIORef (error "uninitialized global 'worldSize'")

{-# NOINLINE globalRPCMetadata #-}
globalRPCMetadata :: IORef Reg.Lookup
globalRPCMetadata = unsafePerformIO$ newIORef (error "uninitialized 'globalRPCMetadata'")

-- HACK: Assume the init action is invoked from the main program thread.
-- We use this to throw exceptions that will really exit the process.
{-# NOINLINE mainThread #-}
mainThread :: IORef ThreadId
mainThread = unsafePerformIO$ newIORef (error "uninitialized global 'mainThread'")

{-# NOINLINE remoteIvarTable #-}
remoteIvarTable :: HotVar (IntMap.IntMap (Payload -> Par ()))
remoteIvarTable = unsafePerformIO$ newHotVar IntMap.empty

type Token = Int64
{-# NOINLINE shutdownChan #-}
shutdownChan :: Chan Token
shutdownChan = unsafePerformIO $ newChan

-----------------------------------------------------------------------------------
-- Debugging Helpers:
-----------------------------------------------------------------------------------

measureIVarTable :: IO (Int, BS.ByteString)
measureIVarTable = do
  tbl <- readIORef remoteIvarTable
  let size = IntMap.size tbl
  return (size, " (Outstanding unreturned work " +++ sho size +++ ", ids:"+++
	          sho (take 5 $ IntMap.keys tbl)+++" )")

-----------------------------------------------------------------------------------
-- Misc Helpers:
-----------------------------------------------------------------------------------

decodeMsg str = 
  case decode str of
    Left err -> errorExitPure$ "ERROR: decoding message from: " ++ show str
    Right x  -> x

-- forkDaemon = forkIO
forkDaemon name action = 
   forkWithExceptions forkOS ("Daemon thread ("++name++")") action
--   forkOS $ catch action
-- 		 (\ (e :: SomeException) -> do
-- 		  hPutStrLn stderr $ "Caught Error in Daemon thread ("++name++"): " ++ show e
-- 		 )

-- | Send to a particular node in the peerTable:
sendTo :: NodeID -> BS.ByteString -> IO ()
sendTo ndid msg = MP.sendBS commWorld ndid 0 msg

-- | This assumes a "Payload closure" as produced by makePayloadClosure below.
deClosure :: Closure Payload -> IO (Par Payload)
-- deClosure :: (Typeable a) => Closure a -> IO (Maybe a)
deClosure pclo@(Closure ident payload) = do 
  dbgTaggedMsg 5$ "Declosuring : "+++ sho ident +++ " type "+++ sho payload
  lkp <- readIORef globalRPCMetadata
  case Reg.getEntryByIdent lkp ident of 
    Nothing -> fail$ "deClosure: failed to deserialize closure identifier: "++show ident
    Just fn -> return (fn payload)

-- TEMP FIXME: INLINING THIS HERE:
makePayloadClosure :: Closure a -> Maybe (Closure Payload)
makePayloadClosure (Closure name arg) = 
                case isSuffixOf "__impl" name of
                  False -> Nothing
                  True -> Just $ Closure (name++"Pl") arg

-- | Try to exit the whole process and not just the thread.
errorExit :: String -> IO a
-- TODO: Might be better to just call "kill" on our own PID:
errorExit str = do
   printErr    $ "ERROR: "++str 
   dbgTaggedMsg 0 $ BS.pack $ "ERROR: "++str 

-- The idea with this debugging hack is to prevent ANYTHING that could be closing connections:
#ifdef TMPDBG
   diverge
#endif
   printErr$ "Connections closed, now exiting process."
   exitProcess 1

printErr = BS.putStrLn . BS.pack 

foreign import ccall "exit" c_exit :: Int -> IO ()

-- | Exit the process successfully.
exitSuccess :: IO a
exitSuccess = do 
   dbgTaggedMsg 1 "  EXITING process with success exit code."
#ifdef TMPDBG
   diverge
#endif
   dbgTaggedMsg 1 "   (Connections closed, now exiting for real)"
   exitProcess 0

--diverge = do putStr "!?"; threadDelay 100; diverge
diverge = do dbgCharMsg 0 "!?" "Purposefully diverging instead of exiting for this experiment..."
	     threadDelay 200
	     diverge

exitProcess :: Int -> IO a
exitProcess code = do
   -- Cleanup: 
   hFlush stdout
   hFlush stderr
   -- Hack... pause for a bit...
   threadDelay (100*1000)
   ----------------------------------------
   -- Option 1: Async exception
   -- tid <- readIORef mainThread
   -- throwTo tid (ErrorCall$ "Exiting with code: "++show code)
   --   NOTE ^^ - this was having problems not being serviced.

   -- Option 2: kill:
   --      mypid <- getProcessID
   --      system$ "kill "++ show mypid

   -- Option 3: FFI call:
   c_exit code

   -- putStrLn$ "SHOULD NOT SEE this... process should have exited already."
   return (error "Should not see this.")

errorExitPure :: String -> a
errorExitPure str = unsafePerformIO$ errorExit str

instance Show Payload where
  show payload = "<type: "++ show (getPayloadType payload) ++", bytes: "
		 ++ show (BS.take 100$ getPayloadContent payload) ++ ">"
    
-----------------------------------------------------------------------------------
-- Main scheduler components (init & steal)
-----------------------------------------------------------------------------------

initAction :: [Reg.RemoteCallMetaData] -> (InitMode -> IO (MP.Rank, Int)) -> InitMode -> InitAction
  -- For now we bake in assumptions about being able to SSH to the machine_list:

initAction metadata initTransport Master = IA ia
  where
    ia topStealAction schedMap = do 
     dbgTaggedMsg 2 "Initializing master..."

     -- Initialize the transport layer:
     (myid, size) <- initTransport Master

     -- Write global mutable variables:
     ----------------------------------------
     dbgTaggedMsg 3 $ "Master node id established as: "+++ showNodeID myid
     writeIORef myNodeID myid
     writeIORef masterID myid
     writeIORef isMaster True
     writeIORef worldSize size
     myThreadId >>= writeIORef mainThread
     
     writeIORef globalRPCMetadata (Reg.registerCalls metadata)
     dbgTaggedMsg 3 "RPC metadata initialized."
     ----------------------------------------

     dbgTaggedMsg 3 "  ... waiting for slaves to connect."
     MP.barrier commWorld
     dbgTaggedMsg 2 "  ... All slaves announced themselves."

     dbgTaggedMsg 2 "  All slaves ready!  Launching receiveDaemon..."
     forkDaemon "ReceiveDaemon"$ receiveDaemon schedMap
     
     MP.bcastSend commWorld myid (encode StartStealing)
     ----------------------------------------
     dbgTaggedMsg 3 "Master initAction returning control to scheduler..."
     return ()

------------------------------------------------------------------------------------------
initAction metadata initTransport Slave = IA ia
  where
    ia topStealAction schedMap = do 
     writeIORef taggedmsg_global_mode "_S"
     dbgTaggedMsg 2 "Init slave: creating connection... " 
     
     (myid, size) <- initTransport Slave
     let masterid = 0

     -- ASSUME init action is called from main thread:
     myThreadId >>= writeIORef mainThread     
     writeIORef globalRPCMetadata (Reg.registerCalls metadata)
     dbgTaggedMsg 3$ "RPC metadata initialized."

     writeIORef myNodeID myid
     writeIORef masterID masterid
     writeIORef worldSize size

     -- All slaves ready and waiting to steal.
     MP.barrier commWorld
     dbgTaggedMsg 2 "  ... All slaves waiting to steal."
     
     -- Don't proceed till we get the go-message from the Master:
     msg <- MP.bcastRecv commWorld masterid
     case decodeMsg msg of 
       StartStealing -> dbgTaggedMsg 1$ "Received 'Go' message from master.  BEGIN STEALING."
       msg           -> errorExit$ "Expecting StartStealing message, received: "++ show msg

     forkDaemon "ReceiveDaemon"$ receiveDaemon schedMap
     dbgTaggedMsg 3$ "Slave initAction returning control to scheduler..."

     return ()

--------------------------------------------------------------------------------

-- We initiate shutdown of the remote Monad par scheduling system by
-- sending a message to our own receiveDaemon.  This can only be
-- called on the master node.
initiateShutdown :: Token -> IO ()
initiateShutdown token = do 
  master <- readHotVar isMaster  
  myid   <- readIORef myNodeID
  unless master $ errorExit$ "initiateShutdown called on non-master node!!"
  sendTo myid (encode$ ShutDown token)

  -- TODO: BLOCK Until shutdown is successful.

-- Block until the shutdown process is successful.
waitForShutdown :: Token -> IO ()
waitForShutdown token = do 
   t <- readChan shutdownChan 
   unless (t == token) $ 
     errorExit$ "Unlikely data race occured!  Two separet distributed Par instances tried to shutdown at the same time." 
   return ()

-- The master tells all workers to quit.
masterShutdown :: Token -> IO ()
masterShutdown token = do
   myid <- readIORef myNodeID
   size <- readIORef worldSize
   ----------------------------------------  
   forM_ [0..size-1] $ \ndid -> do
     let nid = toRank ndid
     unless (nid == myid) $ do 
       sendTo nid (encode$ ShutDown token)
   ----------------------------------------  
#ifdef SHUTDOWNACK
   dbgTaggedMsg 1$ "Waiting for ACKs that all slaves shut down..."
   let loop 0 = return ()
       loop n = do msg <- MP.recvBS commWorld n anyTag
		   case decodeMsg msg of
		     ShutDownACK -> loop (n-1)
		     other -> do 
                                 dbgTaggedMsg 4$ "Warning: received other msg while trying to shutdown.  Might be ok: "++show other
			         loop n
   loop (size - 1)
   dbgTaggedMsg 1$ "All nodes shut down successfully."
#endif
   writeChan shutdownChan token

-- TODO: Timeout.


workerShutdown :: (IntMap.IntMap Sched) -> IO ()
workerShutdown schedMap = do
   dbgTaggedMsg 1$ "Shutdown initiated for worker."
#ifdef SHUTDOWNACK
   mid <- readIORef masterID
   sendTo mid (encode ShutDownACK)
#endif
   -- Because we are completely shutting down the process we are at
   -- liberty to kill all worker threads here:
   forM_ (IntMap.elems schedMap) $ \ Sched{tids} -> do
--     set <- readHotVar tids
     set <- modifyHotVar tids (\set -> (Set.empty,set))
     mapM_ killThread (Set.toList set) 
   dbgTaggedMsg 1$ "  Killed all Par worker threads."
   
   MP.finalize
   exitSuccess

-- Kill own pid:
-- shutDownSelf


--------------------------------------------------------------------------------

stealAction :: StealAction
stealAction = SA sa
  where 
    sa Sched{no} _ = do
      dbgDelay "stealAction"
      -- First try to pop local work:
      x <- R.tryPopR longQueue
      case x of 
        Just (LongWork{localver}) -> do
          dbgTaggedMsg 3$ "stealAction: worker number "+++sho no+++" found work in own queue."
          return (Just localver)
        Nothing -> raidPeer

    pickVictim myid = do
      len <- readHotVar worldSize
      let loop = do 
            n :: Int <- randomIO 
 	    let ind = n `mod` len
 	    if ind == myid 
             then pickVictim myid
	     else return ind
      loop 

    raidPeer = do
      myid <- readHotVar myNodeID
      ind  <- pickVictim (fromRank myid :: Int)
      (_,str) <- measureIVarTable
      dbgTaggedMsg 4$ ""+++ showNodeID myid+++" Attempting steal from Node ID "
                      +++showNodeID ind+++"  "+++ str
      sendTo (toRank ind) (encode$ StealRequest myid)

      -- We have nothing to return immediately, but hopefully work will come back later.
      return Nothing

--------------------------------------------------------------------------------

-- | Spawn a parallel subcomputation that can happen either locally or remotely.
longSpawn (local, clo@(Closure n pld)) = do
  let pclo = fromMaybe (errorExitPure "Could not find Payload closure")
                     $ makePayloadClosure clo
  Sched{no, ivarUID} <- ask

  iv <- new
  liftIO$ do
    dbgTaggedMsg 5$ " Serialized closure: " +++ sho clo
    (cap, _) <- (threadCapability =<< myThreadId)
    dbgTaggedMsg 5$ " [cap "+++ sho cap +++"] longSpawn'ing computation..." 

    -- Create a unique identifier for this IVar that is valid for the
    -- rest of the current run:
--    ivarid <- hashUnique <$> newUnique -- This is not actually safe.  Hashes may collide.

    cntr <- modifyHotVar ivarUID (\ !n -> (n+1,n) )
    -- This should be guaranteed to be unique:
    let ivarid = numCapabilities * cntr + no

    let ivarCont payl = case serialDecodePure payl of 
			  Just x  -> put_ iv x
			  Nothing -> errorExitPure$ "Could not decode payload: "++ show payl

    -- Update the table with a Par computation that can write in the result.
    modifyHotVar_ remoteIvarTable (IntMap.insert ivarid ivarCont)

    R.pushR longQueue 
       (LongWork{ stealver= Just (ivarid,pclo),
		  localver= do x <- local
                               liftIO$ do 
                                  dbgTaggedMsg 4 $ "Executed LOCAL version of longspawn.  "+++
		                                   "Filling & unregistering ivarid "+++sho ivarid
		                  modifyHotVar_ remoteIvarTable (IntMap.delete ivarid)
		               put_ iv x
		})

  return iv


--------------------------------------------------------------------------------

-- | Receive steal requests from other nodes.  This runs in a loop indefinitely.
receiveDaemon :: HotVar (IntMap.IntMap Sched) -> IO ()
receiveDaemon schedMap = 
  do myid <- readIORef myNodeID
     rcvLoop myid
 where 
  rcvLoop myid = do

   dbgDelay "receiveDaemon"
   (_,outstanding) <- measureIVarTable
   dbgTaggedMsg 4$ "[rcvdmn] About to do blocking rcv of next msg... "+++ outstanding

   -- Do a blocking receive to process the next message:
   (bs, _)  <- if False
	   then Control.Exception.catch 
                      (MP.recvBS commWorld anySource anyTag)
		      (\ (e::SomeException) -> do
		       printErr$ "Exception while attempting to receive message in receive loop."
		       exitSuccess
		      )
	   else MP.recvBS commWorld anySource anyTag
   dbgTaggedMsg 4$ "[rcvdmn] Received "+++ sho (BS.length bs) +++" byte message..."

   case decodeMsg bs of 
     StealRequest ndid -> do
       dbgCharMsg 3 "!" ("[rcvdmn] Received StealRequest from: "+++ showNodeID ndid)

       -- There are no "peek" operations currently.  Instead assuming pushL:
       p <- R.tryPopL longQueue
       case p of 
	 Just (LongWork{stealver= Just stealme}) -> do 
	   dbgTaggedMsg 2 "[rcvdmn]   StealRequest: longwork in stock, responding with StealResponse..."
	   sendTo ndid (encode$ StealResponse myid stealme)

          -- TODO: FIXME: Dig deeper into the queue to look for something stealable:
	 Just x -> do 
	    dbgTaggedMsg 2  "[rcvdmn]   StealRequest: Uh oh!  The bottom thing on the queue is not remote-executable.  Requeing it."
            R.pushL longQueue x
	 Nothing -> do
	    dbgTaggedMsg 4  "[rcvdmn]   StealRequest: No work to service request.  Not responding."
            return ()
       rcvLoop myid

     StealResponse fromNd pr@(ivarid,pclo) -> do
       dbgTaggedMsg 3$ "[rcvdmn] Received Steal RESPONSE from "+++showNodeID fromNd+++" "+++ sho pr
       loc <- deClosure pclo
       -- Here the policy is to execute local work (woken
       -- continuations) using the SAME work queue.  We thus have
       -- heterogeneous entries in that queue.
       R.pushL longQueue 
	    (LongWork { stealver = Nothing,
			localver = do 
				     liftIO$ dbgTaggedMsg 1 $ "[rcvdmn] RUNNING STOLEN PAR WORK "
				     payl <- loc
				     liftIO$ dbgTaggedMsg 1 $ "[rcvdmn]   DONE running stolen par work."
				     liftIO$ sendTo fromNd (encode$ WorkFinished myid ivarid payl)
				     return ()
		       })
       rcvLoop myid

     WorkFinished fromNd ivarid payload -> do
       dbgTaggedMsg 2$ "[rcvdmn] Received WorkFinished from "+++showNodeID fromNd+++
		    " ivarID "+++sho ivarid+++" payload: "+++sho payload

       -- table <- readHotVar remoteIvarTable
       table <- modifyHotVar remoteIvarTable (\tbl -> (IntMap.delete ivarid tbl, tbl))

       case IntMap.lookup ivarid table of 
	 Nothing  -> errorExit$ "Processing WorkFinished message, failed to find ivarID in table: "++show ivarid
	 Just parFn -> 
	   R.pushL longQueue (LongWork { stealver = Nothing, 
					 localver = parFn payload
				       })
       rcvLoop myid

     -- This case EXITS the receive loop peacefully:
     ShutDown token -> do
       (num,outstanding) <- measureIVarTable
       dbgTaggedMsg 1 $ "[rcvdmn] -== RECEIVED SHUTDOWN MESSAGE ==-" +++ outstanding
       unless (num == 0) $ errorExit " The number of outstanding longSpawned IVars was NOT zero at shutdown"

       master    <- readHotVar isMaster
       schedMap' <- readHotVar schedMap
       if master then masterShutdown token
                 else workerShutdown schedMap'


--------------------------------------------------------------------------------
-- <boilerplate>

-- In Debug mode we require that IVar contents be Show-able:
#ifdef DEBUG
longSpawn  :: (Show a, NFData a, Serializable a) 
           => ParClosure a -> Par (IVar a)
#else
longSpawn  :: (NFData a, Serializable a) 
           => ParClosure a -> Par (IVar a)

#endif

instance Serialize MP.Rank where
  put i   = Ser.put (fromIntegral i :: Word32)
  get     = liftM fromIntegral (Ser.get :: Ser.Get Word32)

#if 0 
-- Option 1: Use the GHC Generics mechanism to derive these:
-- (default methods would make this easier)
instance Serialize Message where
  put = derivePut 
  get = deriveGet
magic_word :: Int64
magic_word = 98989898989898989
-- Note, these encodings of sums are not efficient!
#else 

type TagTy = Word8

-- Even though this is tedious, because I was running into (toEnum)
-- exceptions with the above I'm going to write this out longhand
-- [2012.02.17]:
instance Serialize Message where
 put StartStealing     =    Ser.put (4::TagTy)
 put (ShutDown tok)    = do Ser.put (5::TagTy)
			    Ser.put tok
 put ShutDownACK       =    Ser.put (6::TagTy)
 put (StealRequest id) = do Ser.put (7::TagTy)
			    Ser.put id
 put (StealResponse id (iv,pay)) = 
                         do Ser.put (8::TagTy)
			    Ser.put id
			    Ser.put iv
			    Ser.put pay
 put (WorkFinished nd iv pay) = 
                         do Ser.put (9::TagTy)
			    Ser.put nd
			    Ser.put iv
			    Ser.put pay
 get = do tag <- Ser.get 
          case tag :: TagTy of 
	    4 -> return StartStealing
	    5 -> ShutDown <$> Ser.get
	    6 -> return ShutDownACK
	    7 -> StealRequest <$> Ser.get
	    8 -> do id <- Ser.get
		    iv <- Ser.get
		    pay <- Ser.get
		    return (StealResponse id (iv,pay))
	    9 -> do nd <- Ser.get
		    iv <- Ser.get
		    pay <- Ser.get
		    return (WorkFinished nd iv pay)
            _ -> errorExitPure$ "Remote.hs: Corrupt message: tag header = "++show tag
#endif      
