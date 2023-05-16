#!/bin/sh
echo "wondershaper limit NIC network speed: $4 Kbps"
PARENT_DIR=$(dirname "$(pwd)")
TOOLS_DIR=$PARENT_DIR/tools
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
printf "wondershaper limit NIC network speed: $4 Kbps for root@192.168.7.%d\n" $i
ssh $IP_PREFIX$i "$TOOLS_DIR/wondershaper-master/wondershaper -a $3 -d $4 -u $4";
} &
done
wait