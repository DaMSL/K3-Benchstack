DROP TABLE IF EXISTS customer_src;
CREATE EXTERNAL TABLE customer_src (
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
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/customer/";

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
  c_custkey    Int,
  c_name       String,
  c_address    String,
  c_nationkey  Int,
  c_phone      String,
  c_acctbal    Double,
  c_mktsegment String,
  c_comments   String
)
PARTITIONED BY (ckey Int);

INSERT INTO customer PARTITION (ckey) select *, c_custkey % 8 from customer_src;

COMPUTE STATS customer;
