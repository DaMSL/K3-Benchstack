DROP TABLE IF EXISTS customer;
CREATE EXTERNAL TABLE customer (
  c_custkey    Int,
  c_name       String,
  c_address    String,
  c_nationkey  Int,
  c_phone      String,
  c_acctbal    Double,
  c_mktsegment String,
  c_comments   String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/customer/";
