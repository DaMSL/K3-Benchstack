#!/bin/bash
QUERY_PATH=$1
QUERY_LIST=$2
NUM_TRIALS=$3
VSQL="vsql -h localhost -p 49156 -U dbadmin dbadmin"

# Query path points to the directory containing the queries (.sql files)
# Query list should be a list of queries to run in the 
#TODO verify usage, take database as parameter, port, etc.

echo "\timing" > /tmp/timing.txt
mkdir vertica_results
rm vertica_results/*

for q in $(cat $QUERY_LIST);
do
  echo "On query: $q"
  for i in $(seq 1 $NUM_TRIALS);
  do
    cat /tmp/timing.txt $QUERY_PATH/$q | $VSQL | grep Time >> vertica_results/$q\_result.txt
  done;
done

rm /tmp/timing.txt
