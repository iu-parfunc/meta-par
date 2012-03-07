{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wall #-}

module Control.Monad.Par.Meta.SharedMemoryAccelerate (
    AccPar
  , runPar
  , runParIO
  , Accelerate.AcceleratePar(..)
  , MonadPar(..)
) where

import Data.Monoid

import Control.Applicative

import Control.Monad.Par.Meta 

import qualified Control.Monad.Par.Meta.Resources.Accelerate as Accelerate
import qualified Control.Monad.Par.Meta.Resources.SharedMemory as SharedMemory

newtype AccPar a = AccPar { unAccPar :: MetaPar a }
  deriving (Functor, Applicative, Monad, MonadPar, Accelerate.AcceleratePar)

tries :: Int
tries = 20

ia :: InitAction
ia = SharedMemory.initAction <> Accelerate.initAction

sa :: StealAction
sa = SharedMemory.stealAction tries <> Accelerate.stealAction

runPar   :: AccPar a -> a
runParIO :: AccPar a -> IO a
runPar   = runMetaPar   ia sa . unAccPar
runParIO = runMetaParIO ia sa . unAccPar