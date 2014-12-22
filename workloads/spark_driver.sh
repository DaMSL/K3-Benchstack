#!/bin/bash

# Point this script at a file listing query class names
# A jar file containing all of those classes
# And the scale factor and number of trials 

# Scale factor is only used for TPCH.
# can be one of 10g, 100g, 250g, 500g, 1t

if [ $# -ne 4 ] 
then
  echo "Usage: $0 class_list jar_file scale_factor num_trials "
  exit 1
fi

CLASSLIST=$1
JARFILE=$2
SF=$3
NUMTRIALS=$4

# Create a folder for the results
mkdir spark_results
rm spark_results/*

for class in $(cat $CLASSLIST);
do
  for i in $(seq 1 $NUMTRIALS);
  do
    echo "Running: $class. Trial $i"
    CMD="/software/spark-1.1.0/bin/spark-submit --master spark://qp-hm1.damsl.cs.jhu.edu:7077 --class $class $JARFILE $SF"
    $CMD | grep 'Elapsed\|Results' >> spark_results/$class\_result; 
  done;
done

