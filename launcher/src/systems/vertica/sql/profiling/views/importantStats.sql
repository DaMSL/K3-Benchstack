-- Clock times or execution times?
DROP VIEW IF EXISTS importantStats;
CREATE VIEW importantStats AS
  SELECT *
  FROM
    clockTimes
  NATURAL JOIN
    clockPercentages
  NATURAL JOIN
    mbReserved;
