#!/bin/bash
cp /hadoop/config/core-site.xml /software/hadoop-2.5.1/etc/hadoop/
cp /hadoop/config/hdfs-site.xml /software/hadoop-2.5.1/etc/hadoop/
mkdir /hadoop/namenode/
if [ "$(ls -A /hadoop/namenode/)" ]; then
    echo "Existing namenode data found... no need to format"
else
    echo "Formatting namenode..."
    /software/hadoop-2.5.1/bin/hdfs namenode -format
fi
/software/hadoop-2.5.1/bin/dfsadmin -setSpaceQuota 10t /
/software/hadoop-2.5.1/bin/hdfs namenode
