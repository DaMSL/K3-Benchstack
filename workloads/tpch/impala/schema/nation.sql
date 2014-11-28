DROP TABLE IF EXISTS nation;
CREATE EXTERNAL TABLE nation (
  n_nationkey Int,
  n_name      String,
  n_regionkey Int,
  n_comments  String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/nation/";
