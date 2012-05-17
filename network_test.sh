#!/bin/bash

trap 'kill $(jobs -p)' EXIT
trap 'kill $(jobs -p)' SIGINT

START_PORT=2000
COUNT=$1

MAX_NUMBER=`expr $COUNT - 1`

HOSTS="";

for i in `seq 0 $MAX_NUMBER`; do 
   PORT=`expr $START_PORT + $i`
   HOSTS="$HOSTS localhost:$PORT" 
done 

PIDS=""

for i in `seq 0 $MAX_NUMBER`; do
   PORT=`expr $START_PORT + $i`
  if [ "$i" == "$MAX_NUMBER" ]; then sleep 3; fi;
  echo "RUNNING:./src/net_test $i $PORT $HOSTS &"
  ./src/net_test $i $PORT $HOSTS  & 
  PIDS="$PIDS $!"
done

echo $PIDS
wait
