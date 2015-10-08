#!/usr/bin/env bash

MASTER=$1
/software/spark-1.2.0/bin/spark-class org.apache.spark.deploy.worker.Worker spark://$MASTER:7077 &
echo $! > /tmp/spark_worker.pid
wait $(cat /tmp/spark_worker.pid)
