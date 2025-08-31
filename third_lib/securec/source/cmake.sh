#!/bin/bash

cd build

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
