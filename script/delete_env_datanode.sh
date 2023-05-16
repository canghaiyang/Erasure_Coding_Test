#!/bin/sh
echo "rm save directory for datanodes..."
PARENT_DIR=$(dirname "$(pwd)")
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
    printf "rm save directory for datanodes: $PARENT_DIR @$IP_PREFIX%d\n" $i
	ssh $IP_PREFIX$i "rm -rf $PARENT_DIR"
} 
done
wait
