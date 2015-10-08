#!/usr/bin/env bash

DOCKER_FLINK_BIN_DIR=/software/flink-0.9.1/bin

. $DOCKER_FLINK_BIN_DIR/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then 
    FLINK_IDENT_STRING="$USER"
fi                                      

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-taskmanager.pid
perfpid=/tmp/flink-perf.pid

test -f $perfpid && echo "Stopping flink perf... `cat $perfpid`" && kill -TERM `cat $perfpid`
