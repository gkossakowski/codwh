#!/bin/bash

mkdir -p logs
rm logs/*

if [ $# -lt 3 ] ; then
  echo "Usage: ./network_test.sh NODES_NUM QUERY_NUM QUERY_FILE"
  echo ""
  echo "for example ./network_test.sh 2 1 queries/q1.ascii"
  exit
fi

START_PORT=2000
COUNT=$1

MAX_NUMBER=`expr $COUNT - 1`

HOSTS="";

for i in `seq 0 $MAX_NUMBER`; do 
   PORT=`expr $START_PORT + $i`
   HOSTS="$HOSTS localhost:$PORT" 
done 

PIDS=""

for i in `seq 1 $MAX_NUMBER`; do
   PORT=`expr $START_PORT + $i`
  echo "RUNNING:./src/worker $i $PORT $HOSTS $2 &"
  ./src/worker $i $PORT $HOSTS $2 > "logs/$i" & # we will remove query um later
  PIDS="$PIDS $!"
done

echo "RUNNING:./src/scheduler 1 queries/q1.ascii 0 $START_PORT $HOSTS &"
sleep 1
./src/scheduler $2 $3 0 $START_PORT $HOSTS > "logs/0" &
PIDS="$PIDS $!"

echo $PIDS
sleep 1

cat `find ./logs`

killall worker
for job in `jobs -p`; do
    wait $job
done
