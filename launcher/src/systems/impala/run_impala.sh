#!/bin/bash

if [ $# -ne 3 ] 
then
  echo "Usage: $0 impala_host scale_factor query_file"
  exit 1
fi

IMPALA_HOST=$1
SF=$2
QUERY_FILE=$3

cat $QUERY_FILE > /tmp/query.sql
echo "SUMMARY;" >> /tmp/query.sql
impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql 2>&1
rm /tmp/query.sql
