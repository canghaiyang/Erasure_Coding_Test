#!/bin/sh
echo "mkdir save directory for datanodes..."
PARENT_DIR=$(dirname "$(pwd)")
CURRENT_DIR=$(pwd)
BUILD_DIR=$PARENT_DIR/build
SCRIPT_DIR=$PARENT_DIR/script
TOOLS_DIR=$PARENT_DIR/tools
LOG_DIR=$PARENT_DIR/log
TEST_FILE_DIR=$PARENT_DIR/test_file
WRITE_DIR=$TEST_FILE_DIR/write
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
    printf "mkdir save directory for datanodes: $BUILD_DIR @$IP_PREFIX%d\n" $i
    ssh $IP_PREFIX$i "mkdir -p $BUILD_DIR"

    printf "mkdir save directory for datanodes: $SCRIPT_DIR @$IP_PREFIX%d\n" $i
    scp -r $SCRIPT_DIR  root@$IP_PREFIX$i:$PARENT_DIR 

    printf "mkdir save directory for datanodes: $TOOLS_DIR @$IP_PREFIX%d\n" $i
    scp -r $TOOLS_DIR  root@$IP_PREFIX$i:$PARENT_DIR 

    printf "mkdir save directory for datanodes: $LOG_DIR @$IP_PREFIX%d\n" $i
    scp -r $LOG_DIR  root@$IP_PREFIX$i:$PARENT_DIR 

    printf "mkdir save directory for datanodes: $WRITE_DIR @$IP_PREFIX%d\n" $i
    ssh $IP_PREFIX$i "mkdir -p $WRITE_DIR"
} 
done
wait

sh $CURRENT_DIR/install_tools.sh $1 $2
