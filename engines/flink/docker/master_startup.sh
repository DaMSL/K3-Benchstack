#!/bin/bash
cp /flink/config/flink-conf.yaml /software/flink-0.7.0-incubating/conf/
/software/flink-0.7.0-incubating/bin/jobmanager.sh start cluster

