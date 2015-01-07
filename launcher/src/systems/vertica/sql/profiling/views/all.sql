DROP VIEW IF EXISTS operatorStats;
CREATE VIEW operatorStats AS
  SELECT *
  FROM
    startEndTimes
  NATURAL JOIN
    startEndPercentages
  NATURAL JOIN
    clockTimes
  NATURAL JOIN
    clockPercentages
  NATURAL JOIN
    executionTimes
  NATURAL JOIN
    executionPercentages
  NATURAL JOIN
    mbReserved
  NATURAL JOIN
    reservedPercentages
  NATURAL JOIN
    mbAllocated
  NATURAL JOIN
    allocatedPercentages;
