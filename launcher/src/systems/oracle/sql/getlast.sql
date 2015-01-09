set long 20000;
set longc 200000;
set lin 1000;
set colsep ',';
set term on;
set serveroutput on;
SELECT extract(second from max(sample_time) - min(sample_time)) as Total_Run_Time
FROM v$active_session_history
WHERE sql_id='&1';
SELECT extract(second from max(sample_time) - min(sample_time)) as Actual_Execute_time
FROM v$active_session_history
WHERE sql_id='&1' AND sql_exec_id is not null;
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
select sql_id, plan_line_id, plan_operation, plan_options, max(workarea_max_mem) as mem from v$sql_plan_monitor where sql_id='br03p3uvm8jzy' group by sql_id, plan_line_id, plan_operation, plan_options  order by plan_line_id) pm
  ,v$active_session_history ash
WHERE pm.plan_line_id = ash.sql_plan_line_id(+)
  AND pm.sql_id = ash.sql_id(+)
--  AND pm.sql_exec_id = ash.sql_exec_id(+)
  AND pm.sql_id='&1'
GROUP BY plan_line_id
  ,plan_operation || ' ' || plan_options
  , mem
ORDER BY plan_line_id
) qstats
,(
  SELECT count(*) AS totalsamples
  FROM v$active_session_history
  WHERE sql_id='&1' and sql_exec_id is not null) ns;
