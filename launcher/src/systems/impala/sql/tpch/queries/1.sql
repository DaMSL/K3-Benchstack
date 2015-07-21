DROP TABLE IF EXISTS tpch_q1_results;

CREATE TABLE tpch_q1_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q1'
AS
  select
          l_returnflag,
          l_linestatus,
          sum(l_quantity) as sum_qty,
          sum(l_extendedprice) as sum_base_price,
          sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
          sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
          avg(l_quantity) as avg_qty,
          avg(l_extendedprice) as avg_price,
          avg(l_discount) as avg_disc,
          count(*) as count_order
  from
          lineitem
  where
          l_shipdate <= "1998-09-02"
  group by
          l_returnflag,
          l_linestatus;
