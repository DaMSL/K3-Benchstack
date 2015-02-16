OPTIONS(DIRECT=TRUE,ROWS=1000000,PARALLEL=TRUE)
LOAD DATA
INFILE "/local/data/tpch10g/orders/*"
 APPEND INTO TABLE orders
 FIELDS terminated by "|"
 (o_orderkey, 
 o_custkey, 
 o_orderstatus, 
 o_totalprice DECIMAL EXTERNAL, 
 o_orderdate  date "YYYY-MM-DD", 
 o_orderpriority, 
 o_clerk, 
 o_shippriority, 
 o_comment)
