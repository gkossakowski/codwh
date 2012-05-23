#!/bin/bash

# Page faults, RSS, Utime
IFACE=eth0
PROC_STAT_PREV=`cat /proc/$1/stat | awk '{ print $13 " " $24 " " $14; }'`
UTIME_PREV=`echo $PROC_STAT_PREV | awk '{ print $3; }'`
CPU_TIME_PREV=`cat /proc/stat | grep "cpu " | awk '{ print $1 + $2 + $3 + $4 + $5 + $6 + $7; }'`
TBYTES_PREV=`cat /proc/net/dev | grep $IFACE | awk '{ print $10; }'`

while true; do
    PROC_STAT=`cat /proc/$1/stat | awk '{ print $13 " " $24 " " $14; }'`
    UTIME=`echo $PROC_STAT | awk '{ print $3; }'`
    CPU_TIME=`cat /proc/stat | grep "cpu " | awk '{ print $1 + $2 + $3 + $4 + $5 + $6 + $7; }'`
    TBYTES=`cat /proc/net/dev | grep $IFACE | awk '{ print $10; }'`
    echo -n "`date +%s` "
    echo -n "$PROC_STAT "
    echo -n $(( (1000 * ($UTIME - $UTIME_PREV) + 5) / ($CPU_TIME - $CPU_TIME_PREV)  ))
    echo " $(( TBYTES - TBYTES_PREV ))"
    PROC_STAT_PREV=$PROC_STAT
    UTIME_PREV=$UTIME
    CPU_TIME_PREV=$CPU_TIME
    TBYTES_PREV=$TBYTES
    sleep 1
done
