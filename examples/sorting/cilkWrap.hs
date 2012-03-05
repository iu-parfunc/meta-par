{-# LANGUAGE ForeignFunctionInterface #-}

import Foreign
import Foreign.C.Types
import System.Environment (getArgs)
import System.Random
import System.CPUTime (getCPUTime)
import Text.Printf

import qualified Data.Vector.Storable as V
import qualified Data.Vector.Storable.Mutable as MS

foreign import ccall unsafe "wrap_cilksort"
  c_cilksort ::  Ptr CLong -> Ptr CLong -> CLong -> IO CLong

foreign import ccall unsafe "run_cilksort"
  c_run_cilksort :: CLong -> IO CLong

main :: IO ()
main =
  do args <- getArgs
     let (size,runs) = case args of
           []    -> (300000, 1)
           [m]   -> (read m, 1)
           [m,n] -> (read m, read n)

     printf "Running cilksort with a size of %d (foreign array) \
        \ (%d runs)\n" (fromIntegral size :: Int) (runs :: Int)

     runCilkSort size runs

     printf "Running cilksort with a size of %d (Haskell array): \
                \ (%d runs).\n" (fromIntegral size :: Int) (runs :: Int)

     -- TODO: would like to do this
     -- runCilkSort' size runs

     seed <- newStdGen

     let v = randomPermutation (fromIntegral size) seed
         t = randomPermutation (fromIntegral size) seed

     mutv <- V.thaw v
     mutt <- V.thaw t

     MS.unsafeWith mutv $ \vptr ->
        MS.unsafeWith mutt $ \tptr ->
        runCilkSort' (castPtr vptr) (castPtr tptr) size runs

     -- for later use, do this:
     -- v' <- V.unsafeFreeze mutv
     -- t' <- V.unsafeFreeze mutt

runCilkSort :: CLong -> Int -> IO ()
runCilkSort _ 0 = return ()
runCilkSort  sz n = do
  ticks <- c_run_cilksort sz
  putStrLn $ "ran in " ++ show ticks ++ " ticks"
  runCilkSort sz (n - 1)

runCilkSort' :: Ptr CLong -> Ptr CLong -> CLong -> Int -> IO ()
runCilkSort' _ _ _ 0 = return ()
runCilkSort' xs ys sz n = do
  ticks <- c_cilksort xs ys sz
  putStrLn $ "ran in " ++ show ticks ++ " ticks"
  runCilkSort' xs ys sz (n - 1)

-- Create a vector containing the numbers [0,N) in random order.
randomPermutation :: Int -> StdGen -> V.Vector Int
randomPermutation len rng =
  -- Annoyingly there is no MV.generate:
  V.create (do v <- V.unsafeThaw$ V.generate len id
               loop 0 v rng)
  -- loop 0 (MV.generate len id)
 where
  loop n vec g | n == len  = return vec
	       | otherwise = do
    let (offset,g') = randomR (0, len - n - 1) g
--    MV.unsafeSwap vec n
    MS.swap vec n (n + offset)
    loop (n+1) vec g'

