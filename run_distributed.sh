#!/bin/bash

if [ $# -lt 2 ] ; then
  echo "Usage: ./run_distributed.sh QUERY_NUM QUERY_FILE"
  echo ""
  echo "for example ./run_distributed.sh 1 queries/q1.ascii"
  echo "for more advanced options (for example username or hosts list)"
  echo "change variables inside."
  exit
fi

SSH="ssh -o StrictHostKeyChecking=no"

USER=rr277637
DOMAIN=".mimuw.edu.pl"
SCHED_HOST="cyan00"
WORK_HOSTS="cyan01 cyan02"

SCHED_CMD="./codwh/src/scheduler"
SCHED_PARAMS="$1 $2 0"
WORK_CMD="./codwh/src/worker"
MONITOR_CMD="./codwh/monitor.sh"

HOSTS="$SCHED_HOST:15000"
WORKER_PORT=15001
for worker in $WORK_HOSTS; do
    HOSTS="$HOSTS $worker:$WORKER_PORT"
    WORKER_PORT=$(( WORKER_PORT + 1 ))
done

i=1
WORKER_PORT=15001
for worker in $WORK_HOSTS; do
    $SSH $USER@$worker$DOMAIN bash -c "\"$WORK_CMD $i $WORKER_PORT $HOSTS $1 2>&1 > worker_$(($WORKER_PORT))_log & echo \\\$! > worker_$(($WORKER_PORT))_pid\"" &
    SSH_PIDS="$SSH_PIDS $!"
    $SSH $USER@$worker$DOMAIN bash -c "\"$MONITOR_CMD \`cat worker_$((WORKER_PORT))_pid\` 2>&1 > monitor_$(($WORKER_PORT))_log & echo \\\$! > monitor_$(($WORKER_PORT))_pid\"" &
    SSH_PIDS="$SSH_PIDS $!"
    i=$(( i + 1 ))
    WORKER_PORT=$(( WORKER_PORT + 1 ))
done

sleep 1

$SSH $USER@$SCHED_HOST$DOMAIN bash -c \""$SCHED_CMD $SCHED_PARAMS 15000 $HOSTS 2>&1 > sched_log & echo \\\$! > sched_pid"\" &
SSH_PIDS="$SSH_PIDS $!"
#$SSH $USER@$worker$DOMAIN bash -c "\"$MONITOR_CMD \`cat sched_pid\` 2>&1 > monitor_15000_log & echo \\\$! > monitor_15000_pid\"" &
#SSH_PIDS="$SSH_PIDS $!"

sleep 1
echo Press Enter to kill workers and gather benchmarking data...
read

#Probably this is not needed, because scheduler dies by itself.
echo Killing scheduler
$SSH $USER@$SCHED_HOST$DOMAIN bash -c \""kill \\\`cat sched_pid\\\`; rm sched_pid "\"

WORKER_PORT=15001
for worker in $WORK_HOSTS; do
    echo Killing worker on $WORKER_PORT
    $SSH $USER@$worker$DOMAIN bash -c \""kill \\\`cat worker_$(($WORKER_PORT))_pid\\\`; rm worker_$(($WORKER_PORT))_pid"\"
    $SSH $USER@$worker$DOMAIN bash -c \""kill \\\`cat monitor_$(($WORKER_PORT))_pid\\\`; rm monitor_$(($WORKER_PORT))_pid"\"
    scp $USER@$worker$DOMAIN:monitor_$(($WORKER_PORT))_log .
    WORKER_PORT=$(( WORKER_PORT + 1 ))
done

#scp $USER@$worker$DOMAIN:sched_log .

for pid in $SSH_PIDS; do
    echo Waiting for ssh session \($pid\)
    wait $pid
done

echo "set multiplot layout $i,1" > gnuplotcmd
#echo "plot \"monitor_15000_log\" using 1:5 with lines, \"monitor_15000_log\" using 1:(\$6/1024) with lines axes x1y2" >> gnuplotcmd
WORKER_PORT=15001
for worker in $WORK_HOSTS; do
    echo "plot \"monitor_$(($WORKER_PORT))_log\" using 1:5 with lines, \"monitor_$(($WORKER_PORT))_log\" using 1:(\$6/1024) with lines axes x1y2" >> gnuplotcmd
    WORKER_PORT=$(( WORKER_PORT + 1 ))
done
echo "unset multiplot" >> gnuplotcmd

cat gnuplotcmd | gnuplot -p
sleep 1
rm gnuplotcmd
