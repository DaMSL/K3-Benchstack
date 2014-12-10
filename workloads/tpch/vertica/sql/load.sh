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
  echo "Usage: $0 data_dir db_name"
  exit 1
fi

DIR=$1
DB=$2
VSQL="vsql $DB"

for tbl in part supplier partsupp customer orders lineitem nation region;
do
  echo "On table: $tbl" 
  cd $DIR/$tbl && cat $(ls $DIR/$tbl) | $VSQL -c "COPY $tbl FROM LOCAL stdin DELIMITER '|' DIRECT;";
done
