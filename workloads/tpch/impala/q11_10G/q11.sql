DROP TABLE IF EXISTS tpch_q11_results;

CREATE EXTERNAL TABLE tpch_q11_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q11'
AS
  select
          ps_partkey,
          sum(ps_supplycost * ps_availqty) as "value"
  from
          partsupp,
          supplier,
          nation
  where
          ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
  group by
          ps_partkey having
                  sum(ps_supplycost * ps_availqty) > 162058275.3049361;
