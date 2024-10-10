#!/bin/bash

ROOT_DIR=$(pwd)
BUILD_DIR=$ROOT_DIR/build
BIN_DIR=$ROOT_DIR/bin

# Check if the build and bin directory exists, if not create it
if [ ! -d "$BUILD_DIR" ]; then
    echo "Creating build directory $BUILD_DIR"
    mkdir -p "$BUILD_DIR"
fi

if [ ! -d "$BIN_DIR" ]; then
    echo "Creating bin directory $BIN_DIR"
    mkdir -p "$BIN_DIR"
fi

cd $BUILD_DIR
cmake ..
make -j 4
cd $ROOT_DIR
