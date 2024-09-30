#!/bin/bash

cd ./src/cmake/build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ../..
make -j 4
cd ../../..
