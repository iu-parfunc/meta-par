{-# LANGUAGE MultiParamTypeClasses #-}
-- TODO: ADD Unsafe

-- | Unsafe operations.  NOT part of "Safe Haskell".
-- 
-- These are "unsafe" (in the normal, Haskell sense) when used with a
-- "runPar" of type `Par a -> a`.  If used with a runPar that stays in
-- the IO monad, then they are simply dangerous.  Unfortunately, there is no good way 

module Control.Monad.Par.Unsafe 
  (
   ParUnsafe(..)
  ) 
where

-- import Control.Monad.Par.Class

-- The class of Par monads that provide unsafe functionality.
-- class ParFuture p iv => ParUnsafe p iv where 
class ParUnsafe p iv where 
  -- | Peek at the current contents of an IVar in a nonblocking way.
  unsafePeek   :: iv a -> p (Maybe a)
  unsafeTryPut :: iv a -> a -> p a


-- If the need ever arises we could also consider unsafeMultiplePut that
-- would be able to change the current value of an IVar.  It could
-- cause big problems in the distributed case, however.
