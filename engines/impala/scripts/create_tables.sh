#!/bin/bash
# Point this script at a folder where each file
# contains CREATE TABLE statements

if [ $# -ne 1 ] 
then
  echo "Usage: $0 schema_folder"
  exit 1
fi

for f in $(ls $1);
do 
  impala-shell -i qp-hm1.damsl.cs.jhu.edu -f $1/$f;
done
