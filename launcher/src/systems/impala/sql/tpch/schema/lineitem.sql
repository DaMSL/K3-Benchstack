DROP TABLE IF EXISTS lineitem_src;
CREATE EXTERNAL TABLE lineitem_src (
  l_orderkey      Int,
  l_partkey       Int,
  l_suppkey       Int,
  l_linenumber    Int,
  l_quantity      Double,
  l_extendedprice Double,
  l_discount      Double,
  l_tax           Double,
  l_returnflag    String,
  l_linestatus    String,
  l_shipdate      String,
  l_commitdate    String,
  l_receiptdate   String,
  l_shipinstruct  String,
  l_shipmode      String,
  l_comments      String
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/lineitem/";

DROP TABLE IF EXISTS lineitem;
CREATE TABLE lineitem (
  l_orderkey      Int,
  l_partkey       Int,
  l_suppkey       Int,
  l_linenumber    Int,
  l_quantity      Double,
  l_extendedprice Double,
  l_discount      Double,
  l_tax           Double,
  l_returnflag    String,
  l_linestatus    String,
  l_shipdate      String,
  l_commitdate    String,
  l_receiptdate   String,
  l_shipinstruct  String,
  l_shipmode      String,
  l_comments      String
); 

insert into lineitem select * from lineitem_src;

COMPUTE STATS lineitem;
