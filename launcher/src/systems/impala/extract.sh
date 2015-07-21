#!/bin/bash

for i in 1 3 5 6 18 22; do
  echo "use 10g;" > /tmp/query.txt
  echo "select * from" >> /tmp/query.txt
  printf "tpch_q%d_results;\n" $i >> /tmp/query.txt
  impala-shell -B --output_delimiter='|' -f /tmp/query.txt > q$i.out  
done

