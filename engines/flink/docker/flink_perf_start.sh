#!/usr/bin/env bash

DOCKER_FLINK_BIN_DIR=/software/flink-0.9.1/bin

. $DOCKER_FLINK_BIN_DIR/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then 
    FLINK_IDENT_STRING="$USER"
fi                                      

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-taskmanager.pid
perfpidfile=/tmp/flink-perf.pid

FREQ=$1
SLEEP=$2

/usr/bin/perfj record -F $FREQ -ag -p `cat $pid` -- sleep $SLEEP &

echo $! > $perfpidfile
echo "Started Flink perf... `cat $perfpidfile`"
