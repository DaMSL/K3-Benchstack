#!/bin/bash

if [ $# -ne 2 ] 
then
  echo "Usage: $0 scale_factor query_file"
  exit 1
fi

SF=$1
QUERY_FILE=$2

cat $QUERY_FILE > /tmp/query.sql
echo "SUMMARY;" >> /tmp/query.sql
impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql 2>&1
#echo 'impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql'
#echo '-----------------------'
#cat /tmp/query.sql
#echo '-----------------------'
rm /tmp/query.sql
