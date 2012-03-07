{-# LANGUAGE CPP #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wall #-}

module Control.Monad.Par.Meta.Resources.Accelerate (
    AcceleratePar(..)
  , initAction
  , stealAction
  ) where

import Control.Applicative
import Control.Concurrent
import Control.Exception.Base (evaluate)
import Control.Monad
import Control.Monad.IO.Class

import Data.Array.Accelerate (Acc, Arrays)
import Data.Array.Accelerate.Array.Sugar
#ifdef ACCELERATE_CUDA_BACKEND
import qualified Data.Array.Accelerate.CUDA as Acc
#else
import qualified Data.Array.Accelerate.Interpreter as Acc
#endif
import Data.Array.Accelerate.IO

import Data.Array.IArray (IArray)
import qualified Data.Array.IArray as IArray    

import qualified Data.Vector.Storable as Vector

import Data.Concurrent.Deque.Class (ConcQueue, WSDeque)
import Data.Concurrent.Deque.Reference as R

import Foreign (Ptr, Storable)

import System.IO.Unsafe

import Text.Printf

import Control.Monad.Par.Meta hiding (dbg, stealAction)

dbg :: Bool
#ifdef DEBUG
dbg = True
#else
dbg = False
#endif

--------------------------------------------------------------------------------
-- Global structures for communicating between Par threads and GPU
-- daemon threads

{-# NOINLINE gpuOnlyQueue #-}
-- | GPU-only queue is pushed to by 'Par' workers on the right, and
-- popped by the GPU daemon on the left. No backstealing is possible
-- from this queue.
gpuOnlyQueue :: WSDeque (IO ())
gpuOnlyQueue = unsafePerformIO R.newQ

{-# NOINLINE gpuBackstealQueue #-}
-- | GPU-only queue is pushed to by 'Par' workers on the right, and
-- popped by the GPU daemon and 'Par' workers on the left.
gpuBackstealQueue :: ConcQueue (MetaPar (), IO ())
gpuBackstealQueue = unsafePerformIO R.newQ

{-# NOINLINE resultQueue #-}
-- | Result queue is pushed to by the GPU daemon, and popped by the
-- 'Par' workers, meaning the 'WSDeque' is appropriate.
resultQueue :: WSDeque (MetaPar ())
resultQueue = unsafePerformIO R.newQ

--------------------------------------------------------------------------------
-- spawnAcc operator and init/steal definitions to export

_spawnAcc :: (Arrays a) => Acc a -> MetaPar (IVar a)
_spawnAcc comp = do 
    when dbg $ liftIO $ printf "spawning Accelerate computation\n"
    iv <- new
    let wrappedComp = do
          when dbg $ printf "running Accelerate computation\n"
          ans <- evaluate $ Acc.run comp
          R.pushL resultQueue $ do
            when dbg $ liftIO $ printf "Accelerate computation finished\n"
            put_ iv ans
    liftIO $ R.pushR gpuOnlyQueue wrappedComp
    return iv               

-- Backstealing variants

-- | Backstealing spawn where the result is converted to an instance
-- of 'IArray'. Since the result of either 'Par' or the 'Acc' version
-- may be put in the resulting 'IVar', it is expected that the result
-- of both computations is an equivalent 'IArray'.
_spawnAccIArray :: ( EltRepr ix ~ EltRepr sh
                   , IArray a e, IArray.Ix ix
                   , Shape sh, Elt ix, Elt e )
                => (MetaPar (a ix e), Acc (Array sh e))
                -> MetaPar (IVar (a ix e))
_spawnAccIArray (parComp, accComp) = do 
    when dbg $ liftIO $ printf "spawning Accelerate computation\n"
    iv <- new
    let wrappedParComp :: MetaPar ()
        wrappedParComp = do
          when dbg $ liftIO $ printf "running backstolen computation\n"
          put_ iv =<< parComp          
        wrappedAccComp = do
          when dbg $ printf "running Accelerate computation\n"
          ans <- evaluate $ Acc.run accComp
          R.pushL resultQueue $ do
            when dbg $ liftIO $ printf "Accelerate computation finished\n"
            put_ iv (toIArray ans)
    liftIO $ R.pushR gpuBackstealQueue (wrappedParComp, wrappedAccComp)
    return iv

-- | Backstealing spawn where the result is converted to a
-- 'Data.Vector.Storable.Vector'. Since the result of either 'Par' or
-- the 'Acc' version may be put in the resulting 'IVar', it is
-- expected that the result of both computations is an equivalent
-- 'Vector'. /TODO/: make a variant with unrestricted 'Shape' that,
-- e.g., yields a vector in row-major order.
_spawnAccVector :: (Storable a, Elt a, BlockPtrs (EltRepr a) ~ ((), Ptr a))
               => (MetaPar (Vector.Vector a), Acc (Array DIM1 a))
               -> MetaPar (IVar (Vector.Vector a))
_spawnAccVector (parComp, accComp) = do 
    when dbg $ liftIO $ printf "spawning Accelerate computation\n"
    iv <- new
    let wrappedParComp :: MetaPar ()
        wrappedParComp = do
          when dbg $ liftIO $ printf "running backstolen computation\n"
          put_ iv =<< parComp
        wrappedAccComp :: IO ()
        wrappedAccComp = do
          when dbg $ printf "running Accelerate computation\n"
          let ans = toVector $ Acc.run accComp
          R.pushL resultQueue $ do
            when dbg $ liftIO $ printf "Accelerate computation finished\n"
            put_ iv ans
    liftIO $ R.pushR gpuBackstealQueue (wrappedParComp, wrappedAccComp)
    return iv

-- runAcc :: (Arrays a) => Acc a -> Par a


-- | Loop for the GPU daemon; repeatedly takes work off the 'gpuQueue'
-- and runs it.
gpuDaemon :: IO ()
gpuDaemon = do
  when dbg $ printf "gpu daemon entering loop\n" 
  mwork <- R.tryPopL gpuOnlyQueue
  case mwork of
    Just work -> work
    Nothing -> do
      mwork2 <- R.tryPopL gpuBackstealQueue
      case mwork2 of
        Just (_, work) -> work 
        Nothing -> return ()
  gpuDaemon

initAction :: InitAction
initAction = IA ia
  where ia _ _ = do
          void $ forkIO gpuDaemon

stealAction :: StealAction
stealAction = SA sa 
  where sa _ _ = do
          mfinished <- R.tryPopR resultQueue
          case mfinished of
            finished@(Just _) -> return finished
            Nothing -> fmap fst `fmap` R.tryPopL gpuBackstealQueue
    
-- New class architecture

class MonadPar m => AcceleratePar m where
  spawnAcc :: (Arrays a) => Acc a -> m (IVar a)
  spawnAccIArray :: ( EltRepr ix ~ EltRepr sh
                    , IArray a e, IArray.Ix ix
                    , Shape sh, Elt ix, Elt e )
                 => (m (a ix e), Acc (Array sh e))
                 -> m (IVar (a ix e))
  spawnAccVector :: (Storable a, Elt a, BlockPtrs (EltRepr a) ~ ((), Ptr a))
                 => (m (Vector.Vector a), Acc (Array DIM1 a))
                 -> m (IVar (Vector.Vector a))

instance AcceleratePar MetaPar where
  spawnAcc       = _spawnAcc
  spawnAccIArray = _spawnAccIArray
  spawnAccVector = _spawnAccVector