DROP TABLE IF EXISTS amplab_q3_results;

CREATE EXTERNAL TABLE amplab_q3_results
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/impala/amplab/q3'
AS
  SELECT sourceIP, totalRevenue, avgPageRank
  FROM
    (SELECT sourceIP,
            AVG(pageRank) as avgPageRank,
            SUM(adRevenue) as totalRevenue
      FROM Rankings AS R, UserVisits AS UV
      WHERE R.pageURL = UV.destURL
         AND UV.visitDate BETWEEN "1980-01-01" AND "1980-04-01" 
      GROUP BY UV.sourceIP) AS t
  ORDER BY totalRevenue DESC LIMIT 1;
