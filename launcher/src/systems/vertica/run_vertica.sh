#!/bin/bash

# Be sure to set the env variables:
# VSQL_HOST 
# VSQL_PORT
# VSQL_USER
# VSQL_DATABASE

if [ $# -ne 2 ] 
then
  echo "Usage: $0 schema_name query_file"
  exit 1
fi

SCHEMA=$1
QUERY_FILE=$2
VSQL="vsql"

# Enable timing and select a search path
echo "\timing" > /tmp/timing.txt
echo "set SEARCH_PATH=$SCHEMA;" > /tmp/set.sql

# TODO write output to log. then grep for result.
cat /tmp/set.sql /tmp/timing.txt $QUERY_FILE | $VSQL | grep Time

rm /tmp/set.sql
rm /tmp/timing.txt
