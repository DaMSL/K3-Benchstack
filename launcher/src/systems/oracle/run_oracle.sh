#!/bin/bash

if [ $# -ne 2 ] 
then
  echo "Usage: $0 db_name query_file"
  exit 1
fi

DB=$1
FILE=$2

echo "quit;" | sqlplus system/manager@$ORACLE_HOST:$ORACLE_PORT/$DB @$FILE | grep 'Elapsed'
