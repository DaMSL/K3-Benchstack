DROP TABLE IF EXISTS tpch_q18_results;

CREATE EXTERNAL TABLE tpch_q18_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q18'
AS
  select 
          c_name,
          c_custkey,
          o_orderkey,
          o_orderdate,
          o_totalprice,
          sum(l_quantity)
  from
          customer,
          orders,
          lineitem
  where
          o_orderkey in (
                  select
                          l_orderkey
                  from
                          lineitem
                  group by
                          l_orderkey having
                                  sum(l_quantity) > 300
          )
          and c_custkey = o_custkey
          and o_orderkey = l_orderkey
  group by
          c_name,
          c_custkey,
          o_orderkey,
          o_orderdate,
          o_totalprice
