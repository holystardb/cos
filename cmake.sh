#!/bin/bash

rm -fr $HOME/cos/build
mkdir -p $HOME/cos/build

rm -fr $HOME/cos/output
mkdir -p $HOME/cos/output
mkdir -p $HOME/cos/output/lib
mkdir -p $HOME/cos/output/bin
mkdir -p $HOME/cos/output/include

cd $HOME/cos/build
cmake .. \
  -DCMAKE_C_COMPILER=/usr/local/gcc-8.2.0/bin/gcc \
  -DCMAKE_CXX_COMPILER=/usr/local/gcc-8.2.0/bin/g++ \
  -DCMAKE_BUILD_TYPE=debug \
  -DCMAKE_INSTALL_PREFIX=$HOME

make clean
make
make install

#cp -fr $HOME/cos/bin $HOME/
#cp -fr $HOME/cos/lib $HOME/
