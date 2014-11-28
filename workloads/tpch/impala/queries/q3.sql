DROP TABLE IF EXISTS tpch_q3_results;

CREATE EXTERNAL TABLE tpch_q3_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q3'
AS
  select  l_orderkey,
          sum(l_extendedprice * (1 - l_discount)) as revenue,
          o_orderdate,
          o_shippriority
  from
          customer,
          orders,
          lineitem
  where
          c_mktsegment = 'BUILDING'
          and c_custkey = o_custkey
          and l_orderkey = o_orderkey
          and o_orderdate < '1995-03-15'
          and l_shipdate  > '1995-03-15'
  group by
          l_orderkey,
          o_orderdate,
          o_shippriority;
