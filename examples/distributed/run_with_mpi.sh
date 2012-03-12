#!/bin/bash

#SBATCH --job-name=monad-par
#SBATCH --ntasks-per-node=1

if [ "$APP" = "" ]; then 
  APP=sumeuler_ported
fi

if [ "$VERBOSITY" = "" ]; then 
  VERBOSITY=0
fi

export VERBOSITY=$VERBOSITY

mpirun -mca btl tcp,self $APP.exe +RTS -N
