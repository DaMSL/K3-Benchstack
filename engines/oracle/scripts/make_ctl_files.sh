#!/bin/bash


# Create region sqlldr control  file
echo 'load data' > region.ctl
echo infile \"$1/region/region*\" >> region.ctl
echo '  append into table region' >> region.ctl
echo '  fields terminated by "|"' >> region.ctl
echo '  (r_regionkey, r_name, r_comment)' >> region.ctl

# Create nation sqlldr control  file
echo 'load data' > nation.ctl
echo   infile \"$1/nation/*\" >> nation.ctl
echo ' into table nation'  >> nation.ctl
echo ' fields terminated by "|"' >> nation.ctl
echo ' (n_nationkey, n_name, n_regionkey, n_comment)' >> nation.ctl

# Create part sqlldr control  file
echo 'load data' > part.ctl
echo   infile \"$1/part/*\" >> part.ctl
echo ' into table part'  >> part.ctl
echo ' fields terminated by "|"' >> part.ctl
echo ' (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)' >> part.ctl

# Create supplier sqlldr control  file
echo 'OPTIONS(DIRECT=TRUE,ROWS=10000)' > supplier.ctl
echo 'load data' >> supplier.ctl
echo   infile \"$1/supplier/*\" >> supplier.ctl
echo ' into table supplier'  >> supplier.ctl
echo ' fields terminated by "|"' >> supplier.ctl
echo ' (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)' >> supplier.ctl

# Create customer sqlldr control  file
echo 'OPTIONS(DIRECT=TRUE,ROWS=100000)' > customer.ctl
echo 'load data' >> customer.ctl
echo   infile \"$1/customer/*\" >> customer.ctl
echo ' into table customer'  >> customer.ctl
echo ' fields terminated by "|"' >> customer.ctl
echo ' (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)' >> customer.ctl

# Create orders sqlldr control  file
echo 'OPTIONS(DIRECT=TRUE,ROWS=1000000)' > orders.ctl
echo 'load data' >> orders.ctl
echo   infile \"$1/orders/*\" >> orders.ctl
echo ' into table orders'  >> orders.ctl
echo ' fields terminated by "|"' >> orders.ctl
echo ' (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate  date "YYYY-MM-DD", o_orderpriority, o_clerk, o_shippriority, o_comment)' >> orders.ctl

# Create partsupp sqlldr control  file
echo 'OPTIONS(DIRECT=TRUE,ROWS=10000000)' > partsupp.ctl
echo 'load data' >> partsupp.ctl
echo   infile \"$1/partsupp/*\" >> partsupp.ctl
echo ' into table partsupp'  >> partsupp.ctl
echo ' fields terminated by "|"' >> partsupp.ctl
echo ' (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)' >> partsupp.ctl

# Create lineitem sqlldr control  file
echo 'OPTIONS(DIRECT=TRUE,ROWS=10000000)' > lineitem.ctl
echo 'load data' >> lineitem.ctl
echo   infile \"$1/lineitem/*\" >> lineitem.ctl
echo ' into table lineitem'  >> lineitem.ctl
echo ' fields terminated by "|"' >> lineitem.ctl
echo ' (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate date "YYYY-MM-DD", l_commitdate date "YYYY-MM-DD", l_receiptdate date "YYYY-MM-DD", l_shipinstruct, l_shipmode, l_comment)' >> lineitem.ctl




