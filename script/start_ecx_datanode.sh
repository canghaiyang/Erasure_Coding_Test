#!/bin/sh
echo "start datanode..."
PARENT_DIR=$(dirname "$1")
LOG_DIR=$PARENT_DIR/log
BUILD_DIR=$PARENT_DIR/build

nohup $BUILD_DIR/ecx_datanode > $LOG_DIR/datanode_log.out 2>&1 &




