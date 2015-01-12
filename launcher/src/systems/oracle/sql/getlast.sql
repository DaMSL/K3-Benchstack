set long 20000;
set longc 200000;
set lin 1000;
set colsep ',';
set term on;
set pages 0;
set serveroutput on;
VARIABLE lastsql VARCHAR2(16);
BEGIN
  SELECT /* @@IGNORE_FLAG@@ */ sql_id
    INTO :lastsql
    FROM v$sql
    WHERE sql_fulltext LIKE '%@@QUERYFLAG@@%'
      AND sql_fulltext NOT LIKE '%@@IGNORE_FLAG@@%'
      AND rownum=1;
END;
/
SELECT extract(second from max(sample_time) - min(sample_time)) as Total_Run_Time
  FROM v$active_session_history
  WHERE sql_id=:lastsql;
SELECT extract(second from max(sample_time) - min(sample_time)) as Actual_Execute_time
  FROM v$active_session_history
  WHERE sql_id=:lastsql AND sql_exec_id is not null;
SELECT qstats.*
,numsamples * 100 AS Time_ms
,round((numsamples / totalsamples * 100), 2) AS PERCENT
FROM (
SELECT plan_line_id
  ,plan_operation || ' ' || plan_options
  ,count(distinct session_id) as parallel_ops
  ,count(ash.sql_plan_line_id) AS numsamples
  ,mem
FROM (
select sql_id, plan_line_id, plan_operation, plan_options, sid, COALESCE(max(workarea_max_mem), 0) as mem from v$sql_plan_monitor where sql_id=:lastsql group by sql_id, plan_line_id, plan_operation, plan_options, sid  order by plan_line_id) pm
  ,v$active_session_history ash
WHERE pm.plan_line_id = ash.sql_plan_line_id(+)
  AND pm.sql_id = ash.sql_id(+)
  AND pm.sid = ash.session_id(+)
--  AND pm.sql_exec_id = ash.sql_exec_id(+)
  AND pm.sql_id=:lastsql
GROUP BY plan_line_id
  ,plan_operation || ' ' || plan_options
  , mem
ORDER BY plan_line_id
) qstats
,(
  SELECT count(*) AS totalsamples
  FROM v$active_session_history
  WHERE sql_id=:lastsql) ns;
