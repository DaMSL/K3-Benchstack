CREATE EXTERNAL TABLE rankings (
  pageURL String, 
  pageRank Int, 
  avgDuration Int
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/";
