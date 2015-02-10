OPTIONS(DIRECT=true,PARALLEL=TRUE)
LOAD DATA
INFILE "/local/data/tpch100g/partsupp/*"
 APPEND INTO TABLE partsupp
 FIELDS terminated by "|"
 (ps_partkey, 
 ps_suppkey, 
 ps_availqty, 
 ps_supplycost DECIMAL EXTERNAL, 
 ps_comment)
