#!/bin/sh
echo "scp send program and scripts..."
PARENT_DIR=$(dirname "$(pwd)")
BUILD_DIR=$PARENT_DIR/build
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
    printf "scp $BUILD_DIR/datanode to root@$IP_PREFIX%d\n" $i
    scp -r $BUILD_DIR/datanode  root@$IP_PREFIX$i:$BUILD_DIR/datanode 
} &
done
wait

