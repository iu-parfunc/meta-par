{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wall #-}

module Control.Monad.Par.Meta.SharedMemoryOnly (
    Par
  , runPar
  , runParIO
  , MonadPar(..)
  , IVar
) where

import Control.Applicative

import Control.Monad.Par.Meta
import qualified Control.Monad.Par.Meta.Resources.SharedMemory as SharedMemory

newtype Par a = Par { unPar :: MetaPar a }
  deriving (Functor, Applicative, Monad, MonadPar)

tries :: Int
tries = 20

ia :: InitAction
ia = SharedMemory.initAction

sa :: StealAction
sa = SharedMemory.stealAction tries

runPar   :: Par a -> a
runParIO :: Par a -> IO a
runPar   = runMetaPar   ia sa . unPar
runParIO = runMetaParIO ia sa . unPar