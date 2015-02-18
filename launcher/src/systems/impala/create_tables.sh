#!/bin/bash

if [ $# -ne 2 ] 
then
  echo "Usage: $0 schema_dir scale_factor"
  exit 1
fi

SCHEMA_DIR=$1
SF=$2

# Drop a database for this SF
impala-shell -i $IMPALA_HOST -q "DROP DATABASE IF EXISTS $SF" 2>&1 >/dev/null

# Create a database for this SF
impala-shell -i $IMPALA_HOST -q "CREATE DATABASE $SF" 2>&1 >/dev/null

# Load the schema
for f in $(ls $SCHEMA_DIR);
do 
  echo "Creating $SCHEMA_DIR/$f"
  sed s/@@SCALE_FACTOR@@/$SF/g $SCHEMA_DIR/$f > /tmp/query.sql
  impala-shell -d $SF -i $IMPALA_HOST -f /tmp/query.sql 2>&1 >/dev/null;
done
rm /tmp/query.sql

echo "Database $SF Created."

