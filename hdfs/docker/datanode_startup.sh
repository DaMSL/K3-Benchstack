#!/bin/bash
cp /hadoop/config/core-site.xml /software/hadoop-2.5.1/etc/hadoop/
cp /hadoop/config/hdfs-site.xml /software/hadoop-2.5.1/etc/hadoop/
mkdir /hadoop/datanode/
/software/hadoop-2.5.1/bin/hdfs datanode
