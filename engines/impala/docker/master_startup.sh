#!/bin/bash
cp /hadoop/config/core-site.xml /etc/impala/conf/
cp /hadoop/config/hdfs-site.xml /etc/impala/conf/
cp /hadoop/config/impala /etc/default/
source /etc/default/impala
statestored &
catalogd 
