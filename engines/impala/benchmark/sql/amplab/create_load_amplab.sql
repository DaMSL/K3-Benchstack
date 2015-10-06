-- For each relation, create an external table suffixed with '_src', then use this table to populate an internal table.

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
