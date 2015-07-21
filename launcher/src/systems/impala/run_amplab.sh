#!/bin/bash

if [ $# -ne 1 ] 
then
  echo "Usage: $0 scale_factor"
  exit 1
fi

SF=$1

for i in 1 2 3; do
  ./run_impala.sh $SF sql/amplab/queries/$i.sql
done
