#!/bin/bash

# Point this script at a workload directory
# with 'queries' and 'schema' sub-directories

# Also provide the scale factor and number of trials

if [ $# -ne 3 ] 
then
  echo "Usage: $0 workload_folder scale_factor num_trials"
  exit 1
fi

SF=$2

# Create a folder for the results
mkdir impala_results
rm impala_results/*

# Load the schema
SCHEMA_DIR=$1/schema
for f in $(ls $SCHEMA_DIR);
do 
  echo "Creating $SCHEMA_DIR/$f"
  sed s/@@SCALE_FACTOR@@/$SF/g $SCHEMA_DIR/$f | impala-shell -i qp-hm1.damsl.cs.jhu.edu
done

# Run each query multiple trials
QUERY_DIR=$1/queries
for q in $(ls $QUERY_DIR);
do
  echo "Running $QUERY_DIR/$q"
  for i in $(seq 1 $3);
  do
    echo "  Trial $i"
    impala-shell -i qp-hm1.damsl.cs.jhu.edu -f $QUERY_DIR/$q 2>&1 >/dev/null | grep 'Fetched' >> impala_results/$q\_result;
  done;
done
