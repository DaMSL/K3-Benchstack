DROP VIEW IF EXISTS startTimes;
CREATE VIEW startTimes AS
  SELECT 
   transaction_id, 
   statement_id, 
   operator_name, 
   localplan_id, to_timestamp((MIN(counter_value) - timestamptz_to_internal(to_timestamp(0)) )/1000000) as start_time
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
     counter_name='start time'
  GROUP BY
    transaction_id, statement_id, operator_name, localplan_id;

DROP VIEW IF EXISTS endTimes;
CREATE VIEW endTimes AS
  SELECT 
   transaction_id, 
   statement_id, 
   operator_name, 
   localplan_id, to_timestamp((MAX(counter_value) - timestamptz_to_internal(to_timestamp(0)) )/1000000) as end_time
  FROM
    EXECUTION_ENGINE_PROFILES
  WHERE
     counter_name='end time'
  GROUP BY
    transaction_id, statement_id, operator_name, localplan_id;

DROP VIEW IF EXISTS startEndTimes;
CREATE VIEW startEndTimes AS
  SELECT
    st.transaction_id,
    st.statement_id,
    st.operator_name,
    st.localplan_id,
    st.start_time,
    et.end_time,
    et.end_time - st.start_time as diff
  FROM    
    startTimes as st
  NATURAL JOIN
    endTimes as et;

DROP VIEW IF EXISTS startEndPercentages;
CREATE VIEW startEndPercentages AS
  SELECT
    seTimes.transaction_id,
    seTimes.statement_id,
    seTimes.operator_name,
    seTimes.localplan_id,
    100 * seTimes.diff / Totals.total as percent_start_end_time
  FROM
    (SELECT transaction_id, statement_id, sum(diff) as total FROM startEndTimes GROUP BY transaction_id, statement_id) as Totals
  NATURAL JOIN
    startEndTimes seTimes;
