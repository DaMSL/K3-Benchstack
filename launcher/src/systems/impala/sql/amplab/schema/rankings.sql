DROP TABLE IF EXISTS rankings_src;
CREATE EXTERNAL TABLE rankings_src (
  pageURL     String,
  pageRank    Int,
  avgDuration Int
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/";

DROP TABLE IF EXISTS rankings;
CREATE TABLE rankings (
  pageURL     String,
  pageRank    Int,
  avgDuration Int
);

INSERT INTO rankings SELECT * FROM rankings_src;

COMPUTE STATS rankings;


