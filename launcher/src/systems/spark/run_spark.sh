#!/bin/bash

# Scale factor is only used for TPCH or KMeans/SGD
# TPCH can be one of 10g, 100g, 250g, 500g, 1t
# Kmeans/SGD can be one of 10g or 100g

if [ $# -ne 3 ] 
then
  echo "Usage: $0 jar_file scale_factor class_name"
  exit 1
fi

JARFILE=$1
SF=$2
CLASS=$3

CMD="/software/spark-1.2.0/bin/spark-submit --master spark://qp-hm1.damsl.cs.jhu.edu:7077 --class $CLASS /build/$JARFILE $SF"
docker run --net=host -v $(pwd):/build damsl/spark $CMD
