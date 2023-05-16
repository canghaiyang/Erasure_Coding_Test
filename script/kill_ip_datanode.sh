#!/bin/sh
echo "kill $1 datanode..."
CURRENT_DIR=$(pwd)
IP_PREFIX=192.168.7.
ssh $IP_PREFIX$1 "$CURRENT_DIR/kill_datanode.sh"
