#!/bin/bash

set -e 
# set -x

if [ "$APP" = "" ]; then 
  APP=parfib_dist
fi

if [ "$DEBUG" != "" ]; then 
  OPTS="-DDEBUG"
else
  OPTS=""
fi

if [ "$GHC" = "" ]; then 
  GHC=ghc
fi

# OBJ=../../dist/build/cbits/pin.o

# -hide-package remote
set -x
$GHC $OBJ -i../..  --make $APP.hs -o $APP.exe -O2 -threaded -rtsopts $OPTS $@
set +x

echo "  Next run this command here:"
echo "MACHINE_LIST=... ./$APP.exe master ARGS"
echo 
echo "  And run this somewhere else:"
echo "./$APP.exe slave ARGS"
