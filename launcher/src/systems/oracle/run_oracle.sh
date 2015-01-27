#!/bin/bash

if [ $# -ne 2 ] 
then
  echo "Usage: $0 db_name query_file"
  exit 1
fi

DB=$1
FILE=$2

now=$(date)

echo "SET FEEDBACK OFF;" > /tmp/oracleq.sql
echo "SET TERM OFF;" >> /tmp/oracleq.sql
cat $FILE >> /tmp/oracleq.sql
echo '/*+ MONITOR PARALLEL */ ' >> /tmp/oracleq.sql
echo "/* $now */;">> /tmp/oracleq.sql
sed "s/@@QUERYFLAG@@/$now/g" systems/oracle/sql/getlast.sql >> /tmp/oracleq.sql

echo "quit;" >> /tmp/oracleq.sql

sqlplus -s system/manager@$ORACLE_HOST:$ORACLE_PORT/$DB @/tmp/oracleq.sql
