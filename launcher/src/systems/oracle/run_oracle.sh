#!/bin/bash

if [ $# -ne 2 ] 
then
  echo "Usage: $0 db_name query_file"
  exit 1
fi

DB=$1
FILE=$2

echo "SET TERM OFF;" >> /tmp/oracleq.sql
cat $FILE >> /tmp/oracleq.sql
cat getlast.sql >> /tmp/oracleq.sql
echo "quit;" >> /tmp/oracleq.sql

sqlplus system/manager@$ORACLE_HOST:$ORACLE_PORT/$DB @/tmp/oracleq.sql

rm /tmp/oracleq.sql
