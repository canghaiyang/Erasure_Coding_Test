#!/bin/sh
echo "test_network_speed... parameter3: 0 parallelism test, 1 seriality test"
IP_PREFIX=192.168.7.

if [ $3 -eq 0 ]
then
   for i in $(seq $1 $2)
   do
   {
   printf "iperf network test: current node up or down root@192.168.7.%d\n" $i
   iperf3 -c $IP_PREFIX$i -t 10
   }&
   done
   wait
else
   for i in $(seq $1 $2)
   do
   {
   printf "iperf network test: current node up or down root@192.168.7.%d\n" $i
   iperf3 -c $IP_PREFIX$i -t 10
   } 
   done
fi



