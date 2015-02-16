DROP TABLE IF EXISTS orders_src;
CREATE EXTERNAL TABLE orders_src (
  o_orderkey      Int,
  o_custkey       Int,
  o_orderstatus   String,
  o_totalprice    Double,
  o_orderdate     String,
  o_orderpriority String,
  o_clerk         String,
  o_shippriority  Int,
  o_comments      String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/orders/";

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  o_orderkey      Int,
  o_custkey       Int,
  o_orderstatus   String,
  o_totalprice    Double,
  o_orderdate     String,
  o_orderpriority String,
  o_clerk         String,
  o_shippriority  Int,
  o_comments      String
);

INSERT INTO orders SELECT * FROM orders_src;

COMPUTE STATS orders;
