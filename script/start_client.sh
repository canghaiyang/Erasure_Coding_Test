#!/bin/sh
echo "start client..."
PARENT_DIR=$(dirname "$(pwd)")
BUILD_DIR=$PARENT_DIR/build

$BUILD_DIR/client $1 $2 $3




