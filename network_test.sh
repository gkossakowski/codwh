#!/bin/bash

mkdir -p logs
rm logs/*

if [ $# -lt 3 ] ; then
  echo "Usage: ./network_test.sh NODES_NUM QUERY_NUM QUERY_FILE"
  echo ""
  echo "for example ./network_test.sh 2 1 queries/q1.ascii"
  exit
fi

USE_VALGRIND=false
START_PORT=4000
COUNT=$1
QUERY_NUM=$2

MAX_NUMBER=`expr $COUNT - 1`
WORKER="./src/worker"
SCHEDULER="./src/scheduler"

if $USE_VALGRIND
then
  VALGRIND="valgrind --leak-check=full -v --suppressions=valgrind.suppressions"
  #Use to generate suppresions
  #VALGRIND="valgrind --leak-check=full --show-reachable=yes --error-limit=no --gen-suppressions=all --log-file=minimalraw.log"

  WORKER="$VALGRIND $WORKER"
  SCHEDULER="$VALGRIND $SCHEDULER"
fi

HOSTS="";

for i in `seq 0 $MAX_NUMBER`; do 
   PORT=`expr $START_PORT + $i`
   HOSTS="$HOSTS localhost:$PORT" 
done 

PIDS=""

for i in `seq 1 $MAX_NUMBER`; do
   PORT=`expr $START_PORT + $i`
  echo "RUNNING:$WORKER $i $QUERY_NUM $PORT $HOSTS &"
  if [ $i -eq 1 ] ; then
  $WORKER $i $PORT $QUERY_NUM $HOSTS &> "logs/`printf \"%03d\" $i`" 3> "logs/results" & # we will remove query um later
  else
  $WORKER $i $PORT $QUERY_NUM $HOSTS &> "logs/`printf \"%03d\" $i`" & # we will remove query um later
  fi
  PIDS="$PIDS $!"
done

echo "RUNNING:$SCHEDULER $QUERY_NUM $3 0 $START_PORT $QUERY_NUM $HOSTS &"
sleep 2
$SCHEDULER $QUERY_NUM $3 0 $START_PORT $QUERY_NUM $HOSTS &> "logs/000" &
PIDS="$PIDS $!"

echo $PIDS
sleep 5

cat `find ./logs -type f | sort`

killall worker

if $USE_VALGRIND
then
  ps aux | grep $USER | grep valgrind | awk '{ print $2; }' | xargs kill
fi

sort logs/results > logs/results_sorted
rm logs/results

for job in `jobs -p`; do
    wait $job
done
