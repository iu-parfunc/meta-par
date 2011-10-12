{-# OPTIONS_GHC -O2 #-}
{-# LANGUAGE TemplateHaskell #-}
import Data.Int
import System.Environment
import Control.Monad.Par
import Control.Monad.IO.Class
-- Testing:
-- import Control.Monad.ParElision
import GHC.Conc

import Remote

type FibType = Int64

parfib1 :: FibType -> Par FibType
parfib1 n | n < 2 = return 1 
parfib1 n = 
  do 
     xf <- longSpawn $ parfib1 (n-1)
     y  <-             parfib1 (n-2)  
     x  <- get xf
     return (x+y)

--------------------------------------------------------------------------------
-- And the wall is hit here: how can we make a value of type Par an
-- instance of 'Binary'?
-- 
-- By default, CloudHaskell allows values of IO a, ProcessM a, and
-- TaskM a to be 'remotable'd, so perhaps we could shoehorn in our
-- particular monad? This privilege for specific monads is already
-- fairly brittle, though, as TH looks for those specific types when
-- it generates remotable code.
--
-- I'm not sure that moving to a direct scheduler where our 'Par' is
-- an instance of MonadIO would help here, since we still will have
-- ContT and ReaderT in the middle of our monad stack, muddying the
-- 'Binary' instance.
--------------------------------------------------------------------------------


$( remotable ['parfib1] )

main = do args <- getArgs	  
          let (version,size) = 
                  case args of 
  		    []    -> ("trace",10)
		    [v,n] -> (v      ,read n)
          remoteInit (Just "config") [Main.__remoteCallMetaData] (initialProcess size)
       
initialProcess size _ = liftIO . print =<< runParDist (parfib1 size) 

{- [2011.03] On 4-core nehalem, 3.33ghz:

  Non-monadic version, real/user time:
  fib(40) 4 threads: 1.1s 4.4s
  fib(42) 1 threads: 9.7s  
  fib(42) 4 threads: 2.86s 11.6s  17GB allocated -- 3.39X
  
     SPARKS: 433784785 (290 converted, 280395620 pruned)

  Monad-par version:
  fib(38) non-threaded: 23.3s 23.1s
  fib(38) 1 thread :    24.7s 24.5s
  fib(38) 4 threads:     8.2s 31.3s

  fib(40) 4 threads:    20.6s 78.6s 240GB allocated


For comparison, Cilkarts Cilk++:
  fib(42) 4 threads:  3.029s 23.610s

Intel Cilk Plus:
  fib(42) 4 threads:  4.212s 16.770s

   1 thread: 17.53 -- 4.16X


------------------------------------------------------------
[2011.03.29] {A bit more measurement}


If I run a CnC/C++ parfib where results are (multiply) written into an
item collection (so there are many insertions, but only a small
number of resident items), then I get these numbers:

  fib(34) 1 thread: 22.78
  fib(34) 4 thread: 13.96 -- 1.63X 

ESTIMATED 3573.76 seconds for fib(42).

-}
