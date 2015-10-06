DROP TABLE IF EXISTS tpch_q6_results;

CREATE TABLE tpch_q6_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q6'
AS
  select
          sum(l_extendedprice * l_discount) as revenue
  from
          lineitem
  where
          l_shipdate >= '1994-01-01'
          and l_shipdate < '1995-01-01'
          and l_discount between 0.06 - 0.01 and 0.06 + 0.01
          and l_quantity < 24;
