#!/bin/bash

#Provide the uri (hdfs://...) 
URI=$1

for i in $(cat folders.txt);
do
  hdfs dfs -fs $URI -mkdir $i;
done
