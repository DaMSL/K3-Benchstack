CREATE EXTERNAL TABLE uservisits (
  sourceIP String,
  destURL String,
  visitDate String,
  adRevenue Double,
  userAgent String,
  coutryCode String,
  languageCode String,
  searchWord String,
  duration Int
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/";
