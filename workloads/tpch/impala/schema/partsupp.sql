DROP TABLE IF EXISTS partsupp;
CREATE EXTERNAL TABLE partsupp (
  ps_partkey    Int,
  ps_suppkey    Int,
  ps_availqty   Int,
  ps_supplycost Double,
  ps_comments   String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/partsupp/";
