#!/bin/bash

# Point this script at a directory full of partitioned tpch data.
# The directory should contain a directory full of chunks for each of the tables.

# Also specify the database inside of vertica to populate

# Be sure to set the env variables:
# VSQL_HOST 
# VSQL_PORT
# VSQL_USER

if [ $# -ne 2 ] 
then
  echo "Usage: $0 data_dir schema_name"
  exit 1
fi

DIR=$1
SCHEMA=$2
VSQL="vsql dbadmin"

echo "set SEARCH_PATH=$SCHEMA;" > /tmp/set.sql
$VSQL -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA"
cat /tmp/set.sql schema.sql | $VSQL

for tbl in part supplier partsupp customer orders lineitem nation region;
do
  echo "On table: $tbl" 
  cd $DIR/$tbl && cat /tmp/set.sql $(ls $DIR/$tbl) | $VSQL -c "SET SEARCH_PATH=$SCHEMA; COPY $tbl FROM LOCAL stdin DELIMITER '|' DIRECT;";
done

rm /tmp/set.sql
