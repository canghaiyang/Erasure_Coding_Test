#!/bin/sh
echo "test_network_speed... parameter3: 0 close server, 1 start server"
IP_PREFIX=192.168.7.

if [ $3 -eq 0 ]
then
   for i in $(seq $1 $2)
   do
   {
   printf "kill iperf server root@192.168.7.%d\n" $i
   ssh $IP_PREFIX$i "pkill -f iperf";
   } &
   done
   wait
else
   for i in $(seq $1 $2)
   do
   {
   printf "start iperf server root@192.168.7.%d\n" $i
   ssh $IP_PREFIX$i "iperf3 -s";
   } &
   done
fi





