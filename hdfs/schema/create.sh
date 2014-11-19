#!/bin/bash
MKDIR=$1

# Provide the prefix of the HDFS mkdir command. (all of the command except the directory to create)
for i in $(cat folders.txt);
do
  $MKDIR $i;
done
