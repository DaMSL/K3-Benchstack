DROP VIEW IF EXISTS mbReserved;
CREATE VIEW mbReserved AS
  SELECT 
    transaction_id,
    statement_id,
    operator_name, 
    path_id, 
    sum(counter_value)/1024/1024 as mb_reserved
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
    counter_name='memory reserved (bytes)'
  group by
    transaction_id, statement_id, operator_name, path_id;

DROP VIEW IF EXISTS mbAllocated;
CREATE VIEW mbAllocated AS
  SELECT 
    transaction_id,
    statement_id,
    operator_name, 
    path_id, 
    sum(counter_value)/1024/1024 as mb_allocated
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
    counter_name='memory allocated (bytes)'
  group by
    transaction_id, statement_id, operator_name, path_id;

DROP VIEW IF EXISTS reservedPercentages;
CREATE VIEW reservedPercentages AS
  SELECT
    mbr.transaction_id,
    mbr.statement_id,
    mbr.operator_name,
    mbr.path_id,
    100 * mbr.mb_reserved / TotalReserved.total as percentReserved
  FROM  
    (SELECT 
       transaction_id, 
       statement_id, 
       sum(mb_reserved) as total 
     FROM 
       mbReserved
     GROUP BY
       transaction_id, statement_id
     ) as TotalReserved
  NATURAL JOIN
    mbReserved as mbr;

DROP VIEW IF EXISTS allocatedPercentages;
CREATE VIEW allocatedPercentages AS
  SELECT
    mba.transaction_id,
    mba.statement_id,
    mba.operator_name,
    mba.path_id,
    100 * mba.mb_allocated / TotalAllocated.total as percentAllocated
  FROM  
    (SELECT 
       transaction_id, 
       statement_id, 
       sum(mb_allocated) as total 
     FROM 
       mbAllocated
     GROUP BY
       transaction_id, statement_id
     ) as TotalAllocated
  NATURAL JOIN
    mbAllocated as mba;
