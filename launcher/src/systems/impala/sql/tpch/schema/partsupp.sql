DROP TABLE IF EXISTS partsupp_src;
CREATE EXTERNAL TABLE partsupp_src (
  ps_partkey    Int,
  ps_suppkey    Int,
  ps_availqty   Int,
  ps_supplycost Double,
  ps_comments   String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/partsupp/";

DROP TABLE IF EXISTS partsupp;
CREATE TABLE partsupp (
  ps_partkey    Int,
  ps_suppkey    Int,
  ps_availqty   Int,
  ps_supplycost Double,
  ps_comments   String
);

INSERT INTO partsupp SELECT * FROM partsupp_src;

COMPUTE STATS partsupp;
