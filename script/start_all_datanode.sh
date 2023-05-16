#!/bin/sh
echo "start all datanodes..."
CURRENT_DIR=$(pwd)
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
    printf "start datanode: root@$IP_PREFIX%d\n" $i
    ssh $IP_PREFIX$i "$CURRENT_DIR/start_datanode.sh $CURRENT_DIR"
} &
done
wait
