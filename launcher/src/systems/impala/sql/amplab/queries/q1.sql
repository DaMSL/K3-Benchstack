DROP TABLE IF EXISTS amplab_q1_results;

CREATE EXTERNAL TABLE amplab_q1_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/amplab/q1'
AS
  SELECT pageURL, pageRank
  FROM rankings
  WHERE pageRank > 1000;
