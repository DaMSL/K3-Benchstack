DROP TABLE IF EXISTS uservisits_src;
CREATE EXTERNAL TABLE uservisits_src (
  sourceIP     String,
  destURL      String,
  visitDate    String,
  adRevenue    Double,
  userAgent    String,
  coutryCode   String,
  languageCode String,
  searchWord   String,
  duration     Int
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/";

DROP TABLE IF EXISTS uservisits;
CREATE TABLE uservisits (
  sourceIP     String,
  destURL      String,
  visitDate    String,
  adRevenue    Double,
  userAgent    String,
  coutryCode   String,
  languageCode String,
  searchWord   String,
  duration     Int
);

INSERT INTO uservisits SELECT * FROM uservisits_src;

COMPUTE STATS uservisits;

