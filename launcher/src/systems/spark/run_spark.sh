#!/bin/bash

# Scale factor is only used for TPCH.
# can be one of 10g, 100g, 250g, 500g, 1t

if [ $# -ne 3 ] 
then
  echo "Usage: $0 jar_file scale_factor class_name"
  exit 1
fi

JARFILE=$1
SF=$2
CLASS=$3

CMD="/software/spark-1.1.0/bin/spark-submit --master spark://$SPARK_HOME:$SPARK_PORT --class $CLASS $JARFILE $SF"
$CMD 2>/dev/null  | grep 'Elapsed'