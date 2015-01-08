DROP TABLE IF EXISTS amplab_q2_results;

CREATE EXTERNAL TABLE amplab_q2_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/amplab/q2'
AS
  SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) 
  FROM uservisits 
  GROUP BY SUBSTR(sourceIP, 1, 8);
