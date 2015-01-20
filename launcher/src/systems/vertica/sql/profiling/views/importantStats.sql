DROP VIEW IF EXISTS importantStats;
CREATE VIEW importantStats AS
  SELECT T.*, o.description
  FROM
    (SELECT *
    FROM
      clockTimes
    NATURAL JOIN
      clockPercentages
    NATURAL JOIN
      mbReserved) as T
  LEFT OUTER JOIN
    objects as o
  on T.transaction_id = o.transaction_id
  and T.statement_id = o.statement_id
  and T.path_id = o.path_id;
