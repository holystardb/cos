cd cos

rm -fr build
mkdir -p build

cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=debug \
  -DCMAKE_INSTALL_PREFIX=/home/cos
make clean
make
make install

cp -fr $HOME/cos/bin $HOME/
cp -fr $HOME/cos/lib $HOME/
