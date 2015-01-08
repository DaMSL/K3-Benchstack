DROP VIEW IF EXISTS clockTimes;  
CREATE VIEW clockTimes AS
  SELECT 
   transaction_id,
   statement_id,
   operator_name, 
   path_id,
   max(counter_value)/1000 as clock_time
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
    counter_name='clock time (us)'
  group by
    transaction_id, statement_id, operator_name, path_id;

DROP VIEW IF EXISTS executionTimes;
CREATE VIEW executionTimes AS
  SELECT 
   transaction_id,
   statement_id,
   operator_name, 
   path_id,
   max(counter_value)/1000 as execution_time
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
    counter_name='execution time (us)'
  group by
    transaction_id, statement_id, operator_name, path_id;

DROP VIEW IF EXISTS clockPercentages;
CREATE VIEW clockPercentages AS
  SELECT
    ct.transaction_id,
    ct.statement_id,
    ct.operator_name,
    ct.path_id,
    100 * ct.clock_time / Totals.total as percent_clock_time
  FROM
    (SELECT transaction_id, statement_id, sum(clock_time) as total
     FROM clockTimes GROUP BY transaction_id, statement_id) as Totals 
  NATURAL JOIN
    clockTimes as ct;

DROP VIEW IF EXISTS executionPercentages;
CREATE VIEW executionPercentages AS
  SELECT
    ct.transaction_id,
    ct.statement_id,
    ct.operator_name,
    ct.path_id,
    100 * ct.execution_time / Totals.total as percent_execution_time
  FROM
    (SELECT transaction_id, statement_id, sum(execution_time) as total
     FROM executionTimes GROUP BY transaction_id, statement_id) as Totals 
  NATURAL JOIN
    executionTimes as ct;
