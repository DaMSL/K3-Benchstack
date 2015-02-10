OPTIONS(DIRECT=TRUE,ROWS=1000000,PARALLEL=TRUE)
LOAD DATA
INFILE "/local/data/tpch100g/customer/*"
 APPEND INTO TABLE customer
 FIELDS terminated by "|"
 (c_custkey, 
 c_name, 
 c_address, 
 c_nationkey, 
 c_phone, 
 c_acctbal DECIMAL EXTERNAL, 
 c_mktsegment, 
 c_comment)
