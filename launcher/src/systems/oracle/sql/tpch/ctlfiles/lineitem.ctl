OPTIONS(DIRECT=TRUE,ROWS=1000000,PARALLEL=TRUE)
LOAD DATA
INFILE "/local/data/tpch10g/lineitem/*"
 APPEND INTO TABLE lineitem
 FIELDS terminated by "|"
 (l_orderkey, 
 l_partkey, 
 l_suppkey, 
 l_linenumber, 
 l_quantity, 
 l_extendedprice DECIMAL EXTERNAL, 
 l_discount DECIMAL EXTERNAL, 
 l_tax DECIMAL EXTERNAL, 
 l_returnflag, 
 l_linestatus, 
 l_shipdate date "YYYY-MM-DD", 
 l_commitdate date "YYYY-MM-DD", 
 l_receiptdate date "YYYY-MM-DD", 
 l_shipinstruct, 
 l_shipmode, 
 l_comment)
