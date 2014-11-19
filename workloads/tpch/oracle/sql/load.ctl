options ( direct=true, parallel=true )

load data
 infile '@@PATH@@/@@PART@@'
 into table part
 fields terminated by "|"
 ( p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@SUPPLIER@@'
 into table supplier
 fields terminated by "|"
 ( s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@PARTSUPP@@'
 into table partsupp
 fields terminated by "|"
 ( ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@CUSTOMER@@'
 into table customer
 fields terminated by "|"
 ( c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@ORDERS@@'
 into table orders
 fields terminated by "|"
 ( o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
   o_orderpriority, o_clerk, o_shippriority, o_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@LINEITEM@@'
 into table lineitem
 fields terminated by "|"
 ( l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
   l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate,
   l_receiptdate, l_shipinstruct, l_shipmode, l_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@NATION@@'
 into table nation
 fields terminated by "|"
 ( n_nationkey, n_name, n_regionkey, n_comment
   terminated by "\r")

load data
 infile '@@PATH@@/@@REGION@@'
 into table region
 fields terminated by "|"
 ( r_regionkey, r_name, r_comment
   terminated by "\r")
