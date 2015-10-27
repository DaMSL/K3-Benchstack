#!/usr/bin/env bash

perfpid=/tmp/flink_worker_perf.pid

test -f $perfpid && pgrep -F $perfpid && echo "Stopping flink perf... `cat $perfpid`" && kill -TERM `cat $perfpid`
