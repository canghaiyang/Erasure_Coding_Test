#!/bin/sh
echo "kill datanode..."
PARENT_DIR=$(dirname "$1")
BUILD_DIR=$PARENT_DIR/build

kill -9 $(pidof ecx_datanode)
fuser -k $BUILD_DIR/ecx_datanode