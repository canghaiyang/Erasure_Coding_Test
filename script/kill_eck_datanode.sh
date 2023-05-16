#!/bin/sh
echo "kill datanode..."
PARENT_DIR=$(dirname "$1")
BUILD_DIR=$PARENT_DIR/build

kill -9 $(pidof eck_datanode)
fuser -k $BUILD_DIR/eck_datanode