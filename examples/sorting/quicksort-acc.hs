{- Started working on a quicksort, since Blelloch's paper talked about this,
 - but still ran into problems...
 - The nVidia paper says that the sequential sort should be a bitonic sort,
 - and maybe that would be easier.
 -}

-- checkSorted - used by quicksort
--checkSorted :: Elt a => Acc (Vector a) -> Acc 
checkSorted arr = Acc.fold (&&*) (lift True) $ Acc.zipWith (<=*) arr' arr 
    where arr' = Acc.prescanl (\x -> id) (arr ! (index1 0)) arr   

seqSort :: Acc (Vector a) -> Acc (Vector a)
seqSort arr = undefined
    where
        -- copy pivot across with scanl
        pivots = scanl const (arr ! 0) arr
        -- array of 0s/1s, where 0 means < pivot, and 1 means >= pivot
        flags = zipWith (\x y -> (x <* y) ? (0,1)) arr pivots
        -- use split to arrange numbers < pivot at the beginning, before
        -- numbers >= pivot
        pivotSort = split arr flags

seqSort' arr flags = undefined
    where
        -- array of pivot values copied across to their resp. segments
        pivotArr = fstA pairPivotsFlags
        -- set flags to 0 or 1, based on comparison to pivots in pivotArr
        flags = zipWith (\x y -> (x <* y) ? (0,1)) arr pivotArr
        pairArrFlags   = zip arr flags
        pairPivotsFlags = scanl1 f pairArrFlags
        -- (Int, flag) -> (Int, flag) -> (Int, flag)
        -- copy first value with a 1 flag across all 0 flags
        -- when a new 1 flag is found, copy that value (flags are unchanged)
        f = \prev next -> let (a, flag) = unlift next
                              (pa, pflag) = unlift prev in
                            if flag ==* 0
                            then (pa, flag)
                            else (a, flag)
 
-- sort arr based on flags, where flags is an array of 0's and 1's, and
-- numbers in arr will be sorted (stably) so that 0's will be at the beginning
-- and 1's at the end
split :: Acc (Vector Int) -> Acc (Vector Int) -> Acc (Vector a)
split arr flags = permute const arr (\i -> index1 (newInds ! i)) arr
    where
        -- choose from iup and idown, depending on whether flag is 0 or 1
        choose f x = let (a,b) = unlift x in (f ==* 0) ? (a,b)
        -- n = array size
        n     = (size arr) - 1
        -- indices for 0's (numbers < pivot)
        idown = prescanl (+) 0 . map (xor 1) $ flags
        -- indices for 1's (numbers >= pivot)
        iup   = map (n -) . prescanr (+) 0   $ flags
        -- calculate new indices, based on 0's and 1's
        newInds = zipWith choose flags (zip idown iup) 


