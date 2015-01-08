#!/bin/bash

# Be sure to set the env variables:
# VSQL_HOST 
# VSQL_PORT
# VSQL_USER
# VSQL_DATABASE

if [ $# -ne 2 ] 
then
  echo "Usage: $0 schema_name(amplab, tpch10g, tpch100g) query_file"
  exit 1
fi

SCHEMA=$1
QUERY_FILE=$2
VSQL="vsql"

# Enable timing and select a search path. Enable profiling.
echo "\timing" > /tmp/timing.txt
echo "set SEARCH_PATH=$SCHEMA; profile " > /tmp/set.sql

# Grep for HINT: find out where to look in the profiling tables
# Grep for Time .
cat /tmp/set.sql /tmp/timing.txt $QUERY_FILE | $VSQL 2>&1 | grep "Time\|HINT"

rm /tmp/set.sql
rm /tmp/timing.txt
