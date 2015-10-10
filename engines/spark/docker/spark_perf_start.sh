#!/usr/bin/env bash

pid=/tmp/spark_worker.pid
perfpid=/tmp/spark_worker_perf.pid

FREQ=$1
OUTPUT=$2
SLEEP=$3

test -f $pid && pgrep -F $pid && /usr/bin/perfj record -F $FREQ -o $OUTPUT -g -p `cat $pid` -- sleep $SLEEP &

if [ $? = 0 ]; then
  sleep 2
  echo `pgrep perf` > $perfpid
  echo "Started Spark perf... `cat $perfpid`"
else
  echo "Failed to start Flink perf monitoring"
fi
