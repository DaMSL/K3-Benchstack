DROP TABLE IF EXISTS supplier;
CREATE EXTERNAL TABLE supplier (
  s_suppkey   Int,
  s_name      String,
  s_address   String,
  s_nationkey Int,
  s_phone     String,
  s_acctbal   Double,
  s_comments  String 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/supplier/";
