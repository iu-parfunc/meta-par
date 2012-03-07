{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wall #-}

module Control.Monad.Par.Meta.NUMAOnly (
    Par                                    
  , runPar
  , runParIO
  , MonadPar(..)
) where

import Control.Applicative

import Control.Monad.Par.Meta
import qualified Control.Monad.Par.Meta.Resources.NUMA as NUMA

newtype Par a = Par { unPar :: MetaPar a }
  deriving (Functor, Applicative, Monad, MonadPar)

tries :: Int
tries = 20

ia :: InitAction
ia = NUMA.initActionFromEnv

sa :: StealAction
sa = NUMA.stealActionFromEnv tries

runPar   :: Par a -> a
runParIO :: Par a -> IO a
runPar   = runMetaPar   ia sa . unPar
runParIO = runMetaParIO ia sa . unPar