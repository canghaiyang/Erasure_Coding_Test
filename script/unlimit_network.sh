#!/bin/sh
echo "wondershaper unlimit NIC network speed"
PARENT_DIR=$(dirname "$(pwd)")
TOOLS_DIR=$PARENT_DIR/tools
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
printf "wondershaper unlimit NIC network speed: for root@192.168.7.%d\n" $i
ssh $IP_PREFIX$i "$TOOLS_DIR/wondershaper-master/wondershaper -c -a $3";
} &
done
wait