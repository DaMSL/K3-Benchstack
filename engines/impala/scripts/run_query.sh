#!/bin/bash
# Provide an input sql file to execute with Impala
# And a path to store the time info

if [ $# -ne 2 ] 
then
  echo "Usage: $0 query_file output_file"
  exit 1
fi

impala-shell -i qp-hm1.damsl.cs.jhu.edu -f $1 2>&1 >/dev/null | grep 'Fetched' > $2
