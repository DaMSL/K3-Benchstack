DROP TABLE IF EXISTS tpch_q22_results;

CREATE EXTERNAL TABLE tpch_q22_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/tpch/q22'
AS
  select
          cntrycode,
          count(*) as numcust,
          sum(c_acctbal) as totacctbal
  from
          (
                  select
                          substring(c_phone,1,2) as cntrycode,
                          c_acctbal
                  from
                          customer
                  where
                          substring(c_phone, 1, 2) in
                                  ('13', '31', '23', '29', '30', '18', '17')
                          and c_acctbal > (
                                  select
                                          avg(c_acctbal)
                                  from
                                          customer
                                  where
                                          c_acctbal > 0.00
                                          and substring(c_phone, 1, 2) in
                                                  ('13', '31', '23', '29', '30', '18', '17')
                          )
                          and not exists (
                                  select
                                          *
                                  from
                                          orders
                                  where
                                          o_custkey = c_custkey
                          )
          ) as custsale
  group by
          cntrycode
