drop table if exists rankings;
create table rankings (
   pageURL       varchar(300)  not null,
   pageRank      integer       not null,
   avgDuration   integer       not null
);


drop table if exists uservisits;
create table uservisits (
   sourceIP     VARCHAR(116) not null,
   destURL      VARCHAR(100) not null,
   visitDate    DATE         not null,
   adRevenue    FLOAT        not null,
   userAgent    VARCHAR(256) not null,
   countryCode  CHAR(3)      not null,
   languageCode CHAR(6)      not null,
   searchWord   VARCHAR(32)  not null,
   duration     integer      not null
);
