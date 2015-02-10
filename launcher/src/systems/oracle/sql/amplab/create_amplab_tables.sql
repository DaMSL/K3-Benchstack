CREATE TABLE rankings (
  pageURL     varchar(300),
  pageRank    integer,
  avgDuration integer
) INMEMORY;


CREATE TABLE uservisits (
  sourceIP     varchar(116),
  destURL      varchar(100),
  visitDate    date,
  adRevenue    numeric,
  userAgent    varchar(256),
  coutryCode   varchar(3),
  languageCode varchar(6),
  searchWord   varchar(32),
  duration     integer
) INMEMORY;
