#!/bin/bash
cp /flink/config/flink-conf.yaml /software/flink-0.9.1/conf/
/software/flink-0.9.1/bin/taskmanager.sh start batch

