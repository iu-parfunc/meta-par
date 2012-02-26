{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
import Data.Array.Accelerate as Acc
import Data.Array.Accelerate.Interpreter (run)
import Prelude           hiding (zip, map, scanl, scanr, zipWith, fst, scanl1)
import Data.Array.IArray hiding ((!))
import Data.Array.Unboxed       (UArray, IArray)
import Data.Array.IO            (MArray, IOUArray)
import qualified Data.Array.MArray as M

--import Control.Monad.Par.Meta.SharedMemoryAccelerate

import Control.Exception  (evaluate)
import System.Environment (getArgs)
import System.Random ()
import System.Random.MWC  (uniformR, withSystemRandom, Variate, GenIO)

{-
main :: IO ()
main = withSystemRandom $ \gen -> do
    args <- getArgs
    let n = case args of
              []  -> 10000 :: Int
              [n] -> read n
              _   -> error "usage: mergesort_acc.exe [size]"

    vec <- randomUArrayR (minBound,maxBound) gen n
    vec' <- convertUArray vec
    print $ run $ sortAcc vec'
-}
    {-
    print $ run_par vec' ()
    where
        run_par xs () = sortAcc xs
        run_par xs () = runPar $ do
            ans <- spawnAcc $ run_acc (xs :: Vector Int) ()
            get ans
        run_acc xs () = sortAcc xs
        -}


-- Generate a random, uniformly distributed vector of specified size over the
-- range. From Hackage documentation on uniformR: For integral types the 
-- range is inclusive. For floating point numbers, the range (a,b] is used, 
-- if one ignores rounding errors.
randomUArrayR :: (Int, Int) -> GenIO -> Int -> IO (UArray Int Int)
randomUArrayR lim gen n = do
    mu <- M.newArray_ (0,n-1) :: MArray IOUArray e IO => IO (IOUArray Int e)
    let go !i | i < n     = uniformR lim gen >>= M.writeArray mu i >> go (i+1)
              | otherwise = M.unsafeFreeze mu
    go 0

-- Convert an Unboxed Data.Array to an Accelerate Array
convertUArray :: UArray Int Int -> IO (Acc.Vector Int)
convertUArray v = 
    let arr = Acc.fromIArray v in
        evaluate (arr `Acc.indexArray` (Z:.0)) >> return arr

{- 
 - Broad overview:
 -   1. Split data into k blocks
 -   2. Sort each block in parallel with bitonic sort
 -   3. Perform log k steps of pairwise merging
 -      a. To merge two blocks, sample several every 256th element from each 
 -         block
 -      b. Compute the ranks of each sample in each array being merged:
 -          i.  the rank in its own array is just its index
 -          ii. the rank in the other array is found by binary search, which 
 -              can be bounded using knowledge about samples chosen from that 
 -              array
 -      c. These ranks define the boundaries of the sub-blocks of the final 
 -         result
 -      d. Using this, compute the rank of each element, which will be the sum 
 -         of its ranks in the two sub-blocks being merged
 -          i.  the rank in its own array is its index
 -          ii. the rank in the other array is found by binary search (using 
 -              map)
 -      e. Scatter: each thread (from the previous step) writes out its 
 -         element to the correct position. 
 -
 - NOTE: The bin-searching can be expensive, so make sure it's done in on-chip 
 -       shared memory (hence, the 256-element limit).
 -}
-- reduce = foldl
--sortAcc :: Elt a => Vector a -> Acc (Vector a)
--sortAcc k arr = 
    
xor 1 1 = 0
xor 0 1 = 1
xor 1 0 = 1
xor 0 0 = 0 

-- Scatter: takes an array of elements and an array of their destination 
--   indices. Uses Acc.permute to move each element from the original array to 
--   the specified index.
scatter :: Elt a => Acc (Vector Int) -> Acc (Vector a) -> Acc (Vector a)
scatter ind orig = Acc.permute const orig fetchInd orig
    where fetchInd x = index1 (ind ! x)


mergesort :: Acc (Vector Int) -> Acc (Vector Int)
mergesort vec = loop vec segments
  where
    len = size vec
    -- making segments array: each segment should be of size 256 (except the
    -- last one if the number of elements isn't evenly divisible by 256)
    -- seglen = (len `div` 256) + (Acc.min 1 (len `mod` 256))
    seglen = ceilDiv len 256
    segments = generate (index1 seglen)
                        (\i -> ((unindex1 i) /=* (seglen - 1)) ? 
                                (256, 
                                 (len `mod` 256) ==* 0 ? (256, len `mod` 256)))
    sortedVec = bitonicSort vec segments
    
    loop vec segments = cond ((size segments) ==* 1)
                             vec
                             (loop (mergeSegPairs vec segments) 
                                   (mergeSegs segments))

-- Given segment array, merge adjacent pairs (via addition) to make the new 
-- segment array. E.g., [256,256,256,232] ==> [512, 488]
-- TODO: Fix so that this works for odd length arrays
mergeSegs :: Acc (Vector Int) -> Acc (Vector Int)
mergeSegs segs = newSegs
  where
    len = size segs
    -- newlen = (len `div` 2) + (Acc.min 1 (len `mod` 2))
    newlen = ceilDiv len 2
    newSegs = generate (index1 newlen)
                       (\i -> let indbase = 2 * (unindex1 i)
                                  ind0 = index1 $ indbase
                                  ind1 = index1 $ indbase + 1 in
                                (segs ! ind0) + (segs ! ind1))


{- mergeSegPairs - procedure that takes an array and its segments (each
 - segment should be sorted), and merges pairs of segments
 -
 - I see how to find out how many samples per segment, and from that, can make
 - a list of the appropriate size to hold the samples, but I'm not sure how to
 - do all of this, given that there are already segments present. How to do
 - this per pair of segments?
 -}
mergeSegPairs vec segs = undefined
  where
    -- number of samples per segment
    sampleSizes = map (`ceilDiv` 256) segs
    -- would it help to get a list of the indices of the samples?
    

-- TODO: might want to actually sort some stuff here...
bitonicSort v s = v

-- basically, division rounded up
ceilDiv :: Exp Int -> Exp Int -> Exp Int
ceilDiv a b = (a `div` b) + (Acc.min 1 (a `mod` b))

-- Simple Accelerate example, not used at all
-- fold and zipWith are from Data.Array.Accelerate, not Prelude
dotp :: Vector Float -> Vector Float -> Acc (Scalar Float)
dotp xs ys = let xs' = use xs
                 ys' = use ys in
             fold (+) 0 (zipWith (*) xs' ys')


