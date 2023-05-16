#!/bin/sh
echo "intall tools"
CURRENT_DIR=$(pwd)
PARENT_DIR=$(dirname "$(pwd)")
TOOLS_DIR=$PARENT_DIR/tools
IP_PREFIX=192.168.7.

for i in $(seq $1 $2)
do
{
printf "install iperf3 and wondershaper...: root@192.168.7.%d\n" $i
ssh $IP_PREFIX$i "cd $TOOLS_DIR && tar zxvf iperf-master.tar.gz && tar zxvf wondershaper-master.tar.gz"
ssh $IP_PREFIX$i "cd $TOOLS_DIR/iperf-master && ./configure && ldconfig /usr/local/lib && make && make install"
} &
done
wait

