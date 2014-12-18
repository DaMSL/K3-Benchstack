#!/bin/bash

# Query path points to the directory containing the queries (.sql files)
# Query list should be a list of queries to run in the 

# Be sure to set the env variables:
# VSQL_HOST 
# VSQL_PORT
# VSQL_USER

if [ $# -ne 4 ] 
then
  echo "Usage: $0 query_folder query_list schema_name num_trials"
  exit 1
fi

QUERY_PATH=$1
QUERY_LIST=$2
SCHEMA=$3
NUM_TRIALS=$4
VSQL="vsql dbadmin"

echo "\timing" > /tmp/timing.txt
echo "set SEARCH_PATH=$SCHEMA;" > /tmp/set.sql
mkdir vertica_results
rm vertica_results/*

for q in $(cat $QUERY_LIST);
do
  echo "On query: $q"
  for i in $(seq 1 $NUM_TRIALS);
  do
    echo "  Trial $i"
    cat  /tmp/set.sql /tmp/timing.txt $QUERY_PATH/$q | $VSQL | grep Time >> vertica_results/$q\_result.txt
  done;
done

rm /tmp/set.sql
rm /tmp/timing.txt
