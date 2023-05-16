#!/bin/sh
echo "scp send program and scripts..."
PARENT_DIR=$(dirname "$(pwd)")
BUILD_DIR=$PARENT_DIR/build
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
    printf "scp $BUILD_DIR/eck_datanode to root@$IP_PREFIX%d\n" $i
    scp -r $BUILD_DIR/eck_datanode  root@$IP_PREFIX$i:$BUILD_DIR/eck_datanode
} &
done
wait

for i in $(seq $3 $4)
do
{
    printf "scp $BUILD_DIR/ecx_datanode to root@$IP_PREFIX%d\n" $i
    scp -r $BUILD_DIR/ecx_datanode  root@$IP_PREFIX$i:$BUILD_DIR/ecx_datanode
} &
done
wait
