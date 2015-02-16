OPTIONS(DIRECT=TRUE,ROWS=100000,PARALLEL=TRUE)
LOAD data
INFILE "/local/data/tpch100g/supplier/*"
 APPEND INTO TABLE supplier
 FIELDS terminated by "|"
 (s_suppkey, 
 s_name, 
 s_address, 
 s_nationkey, 
 s_phone, 
 s_acctbal DECIMAL EXTERNAL, 
 s_comment)
