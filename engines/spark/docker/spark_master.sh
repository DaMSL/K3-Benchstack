#!/usr/bin/env bash

/software/spark-1.2.0/bin/spark-class org.apache.spark.deploy.master.Master --ip $1 &
echo $! > /tmp/spark_master.pid
wait $(cat /tmp/spark_master.pid)
