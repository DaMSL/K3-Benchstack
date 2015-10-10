#!/usr/bin/env bash

DOCKER_FLINK_BIN_DIR=/software/flink-0.9.1/bin

. $DOCKER_FLINK_BIN_DIR/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then 
    FLINK_IDENT_STRING="$USER"
fi                                      

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-taskmanager.pid
perfpidfile=/tmp/flink_worker_perf.pid

FREQ=$1
OUTPUT=$2
SLEEP=$3

test -f $pid && pgrep -F $pid && /usr/bin/perfj record -F $FREQ -o $OUTPUT -g -p `cat $pid` -- sleep $SLEEP &

if [ $? = 0 ]; then
  sleep 2
  echo `pgrep perf` > $perfpidfile
  echo "Started Flink perf... `cat $perfpidfile`"
else
  echo "Failed to start Flink perf monitoring"
fi
