DROP TABLE IF EXISTS region;
CREATE EXTERNAL TABLE region (
  r_regionkey Int,
  r_name      String,
  r_comments  String
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/region/";
