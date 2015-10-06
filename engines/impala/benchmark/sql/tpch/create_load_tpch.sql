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
);

INSERT INTO customer select * from customer_src;

COMPUTE STATS customer;

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

DROP TABLE IF EXISTS supplier_src;
CREATE EXTERNAL TABLE supplier_src (
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

DROP TABLE IF EXISTS supplier;
CREATE TABLE supplier (
  s_suppkey   Int,
  s_name      String,
  s_address   String,
  s_nationkey Int,
  s_phone     String,
  s_acctbal   Double,
  s_comments  String 
);

INSERT INTO supplier SELECT * from supplier_src;

COMPUTE STATS supplier;

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

DROP TABLE IF EXISTS nation;
CREATE EXTERNAL TABLE nation (
  n_nationkey Int,
  n_name      String,
  n_regionkey Int,
  n_comments  String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/nation/";

DROP TABLE IF EXISTS region;
CREATE EXTERNAL TABLE region (
  r_regionkey Int,
  r_name      String,
  r_comments  String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/@@SCALE_FACTOR@@/region/";
