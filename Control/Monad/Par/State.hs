{-# LANGUAGE ScopedTypeVariables, FlexibleInstances, 
     MultiParamTypeClasses, UndecidableInstances
  #-}

-- | This module provides a notion of (Splittable) State that is
--   compatible with any Par monad.


module Control.Monad.Par.State 
  (
   SplittableState(..)
  )
  where

import Control.Monad
import qualified Control.Monad.Par.Class as PC
import Control.Monad.Trans
import qualified Control.Monad.Trans.State.Strict as S
import qualified Control.Monad.Trans.State.Lazy as SL

---------------------------------------------------------------------------------
--- Make Par computations with state work.
--- (TODO: move these instances to a different module.)

-- | A type in `SplittableState` is meant to be added as to a Par monad
--   using StateT.  It works like any other state except at `fork`
--   points, where the runtime system splits the state using `splitState`.
-- 
--   Common examples for applications of `SplittableState` would
--   include (1) routing a splittable random number generator through
--   a parallel computation, and (2) keeping a tree-index that locates
--   the current computation within the binary tree of `fork`s.
class SplittableState a where
  splitState :: a -> (a,a)

----------------------------------------------------------------------------------------------------
-- Strict State:

-- | Adding State to a `ParFuture` monad yield s another `ParFuture` monad.
instance (SplittableState s, PC.ParFuture p fut) 
      =>  PC.ParFuture (S.StateT s p) fut 
 where
  get = lift . PC.get
  spawn_ (task :: S.StateT s p ans) = 
    do s <- S.get 
       let (s1,s2) = splitState s
       S.put s2                               -- Parent comp. gets one branch.
       lift$ PC.spawn_ $ S.evalStateT task s1   -- Child the other.

-- | Likewise, adding State to a `ParIVar` monad yield s another `ParIVar` monad.
instance (SplittableState s, PC.ParIVar p iv) 
      =>  PC.ParIVar (S.StateT s p) iv 
 where
  fork (task :: S.StateT s p ()) = 
              do s <- S.get 
                 let (s1,s2) = splitState s
                 S.put s2
                 lift$ PC.fork $ do S.runStateT task s1; return ()

  new      = lift PC.new
  put_ v x = lift$ PC.put_ v x
  newFull_ = lift . PC.newFull_

-- | Likewise, adding State to a `ParChan` monad yield s another `ParChan` monad.
instance (SplittableState s, PC.ParChan p snd rcv) 
      =>  PC.ParChan (S.StateT s p) snd rcv
 where
   newChan  = lift   PC.newChan
   recv   r = lift $ PC.recv r
   send s x = lift $ PC.send s x


----------------------------------------------------------------------------------------------------
-- Lazy State:

-- <DUPLICATE_CODE>

-- | Adding State to a `ParFuture` monad yield s another `ParFuture` monad.
instance (SplittableState s, PC.ParFuture p fut) 
      =>  PC.ParFuture (SL.StateT s p) fut 
 where
  get = lift . PC.get
  spawn_ (task :: SL.StateT s p ans) = 
    do s <- SL.get 
       let (s1,s2) = splitState s
       SL.put s2                               -- Parent comp. gets one branch.
       lift$ PC.spawn_ $ SL.evalStateT task s1   -- Child the other.

-- | Likewise, adding State to a `ParIVar` monad yield s another `ParIVar` monad.
instance (SplittableState s, PC.ParIVar p iv) 
      =>  PC.ParIVar (SL.StateT s p) iv 
 where
  fork (task :: SL.StateT s p ()) = 
              do s <- SL.get 
                 let (s1,s2) = splitState s
                 SL.put s2
                 lift$ PC.fork $ do SL.runStateT task s1; return ()

  new      = lift PC.new
  put_ v x = lift$ PC.put_ v x
  newFull_ = lift . PC.newFull_

-- | Likewise, adding State to a `ParChan` monad yield s another `ParChan` monad.
instance (SplittableState s, PC.ParChan p snd rcv) 
      =>  PC.ParChan (SL.StateT s p) snd rcv
 where
   newChan  = lift   PC.newChan
   recv   r = lift $ PC.recv r
   send s x = lift $ PC.send s x

-- </DUPLICATE_CODE>