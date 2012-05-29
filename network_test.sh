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
QUERY_NUM=$2

MAX_NUMBER=`expr $COUNT - 1`

HOSTS="";

for i in `seq 0 $MAX_NUMBER`; do 
   PORT=`expr $START_PORT + $i`
   HOSTS="$HOSTS localhost:$PORT" 
done 

PIDS=""

for i in `seq 1 $MAX_NUMBER`; do
   PORT=`expr $START_PORT + $i`
  echo "RUNNING:./src/worker $i $QUERY_NUM $PORT $HOSTS &"
  if [ $i -eq 1 ] ; then
  ./src/worker $i $PORT $QUERY_NUM $HOSTS &> "logs/`printf \"%03d\" $i`" 3> "logs/results" & # we will remove query um later
  else
  ./src/worker $i $PORT $QUERY_NUM $HOSTS &> "logs/`printf \"%03d\" $i`" & # we will remove query um later
  fi
  PIDS="$PIDS $!"
done

echo "RUNNING:./src/scheduler $QUERY_NUM $3 0 $START_PORT $QUERY_NUM $HOSTS &"
sleep 2
./src/scheduler $QUERY_NUM $3 0 $START_PORT $QUERY_NUM $HOSTS &> "logs/000" &
PIDS="$PIDS $!"

echo $PIDS
sleep 2

cat `find ./logs -type f | sort`

killall worker

sort logs/results > logs/results_sorted
rm logs/results

for job in `jobs -p`; do
    wait $job
done
