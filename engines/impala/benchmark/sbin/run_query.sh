#!/bin/bash

if [ $# -ne 3 ] 
then
  echo "Usage: $0 impala_host query_file scale_factor"
  exit 1
fi

IMPALA_HOST=$1
QUERY_FILE=$2
SF=$3

cd /build
echo "QUERY FILE: $QUERY_FILE"
cat $QUERY_FILE
cat $QUERY_FILE > /tmp/query.sql
echo "SUMMARY;" >> /tmp/query.sql
impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql 2>&1
rm /tmp/query.sql
