DROP VIEW IF EXISTS importantStats;
CREATE VIEW importantStats AS
    SELECT *
    FROM
      clockTimes
    NATURAL JOIN
      clockPercentages
    NATURAL JOIN
      mbReserved;


DROP VIEW IF EXISTS foo;
CREATE VIEW foo AS
    select 
      transaction_id,
      statement_id,
      path_id, 
      sum(clock_time) as clock_time, 
      sum(mb_reserved) as mb_reserved 
    from importantStats 
    group by transaction_id, statement_id, path_id;


DROP VIEW IF EXISTS materializationStats;
CREATE VIEW materializationStats AS
  SELECT foo.*, T.ops, o.description
  FROM
    foo
  NATURAL JOIN
      ops as T
  LEFT OUTER JOIN
    objects as o
  on T.transaction_id = o.transaction_id
  and T.statement_id = o.statement_id
  and T.path_id = o.path_id;
