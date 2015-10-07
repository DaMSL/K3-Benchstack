#!/bin/bash

if [ $# -ne 3 ] 
then
  echo "Usage: $0 impala_host schema_file scale_factor"
  exit 1
fi

IMPALA_HOST=$1
SCHEMA_FILE=$2
SF=$3

cd /build

# Drop a database for this SF
impala-shell -i $IMPALA_HOST -q "DROP DATABASE IF EXISTS $SF" 2>&1 >/dev/null

# Create a database for this SF
impala-shell -i $IMPALA_HOST -q "CREATE DATABASE $SF" 2>&1 >/dev/null

# Load the schema
echo "Creating $SCHEMA_FILE"
sed s/@@SCALE_FACTOR@@/$SF/g $SCHEMA_FILE > /tmp/query.sql
impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql 2>&1 >/dev/null;
rm /tmp/query.sql

echo "Database $SF Created."
