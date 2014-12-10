#!/bin/bash

# Point this script at a directory full of partitioned tpch data.
# The directory should contain a directory full of chunks for each of the tables.
# TODO verify usage, and take database as a parameter.
# We could use a different database for each of the scale factors

DIR=$1
VSQL="vsql -h localhost -p 49156 -U dbadmin dbadmin"
for tbl in supplier partsupp customer orders lineitem nation region;
do
  echo "On table: $tbl" 
  cd $DIR/$tbl && cat $(ls $DIR/$tbl) | $VSQL -c "COPY $tbl FROM LOCAL stdin DELIMITER '|' DIRECT;";
done
