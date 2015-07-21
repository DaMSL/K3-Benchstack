#!/bin/bash

if [ $# -ne 1 ] 
then
  echo "Usage: $0 scale_factor"
  exit 1
fi

SF=$1

for i in 1 3 5 6 18 22; do
  ./run_impala.sh $SF sql/tpch/queries/$i.sql
done
