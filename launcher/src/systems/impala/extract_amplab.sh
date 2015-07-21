#!/bin/bash

for i in 1 2 3; do
  echo "use sf5;" > /tmp/query.txt
  echo "select * from" >> /tmp/query.txt
  printf "amplab_q%d_results;\n" $i >> /tmp/query.txt
  impala-shell -B --output_delimiter='|' -f /tmp/query.txt > q$i.out  
done

