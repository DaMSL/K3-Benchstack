set pages 0;
set lin 2000;
set long 20000;
set longc 200000;
set colsep ',';
set term on
VARIABLE lastsql VARCHAR2(16);
BEGIN
  SELECT prev_sql_id 
    INTO :lastsql
    FROM v$session s, v$sqlarea a 
    WHERE s.sql_address=a.address AND s.sql_hash_value=a.hash_value and rownum=1;  
END;
/
SELECT qstats.*
,numsamples * 100 AS Time_ms
,round((numsamples / totalsamples * 100), 2) AS PERCENT
FROM (
SELECT plan_line_id
  ,plan_operation || ' ' || plan_options
  ,plan_object_name
  ,count(ash.sql_plan_line_id) AS numsamples
  ,max(workarea_max_mem)
FROM v$sql_plan_monitor pm
  ,v$active_session_history ash
WHERE pm.plan_line_id = ash.sql_plan_line_id(+)
  AND pm.sql_id = ash.sql_id(+)
  AND pm.sql_exec_id = ash.sql_exec_id(+)
  AND pm.sql_id = :lastsql
GROUP BY plan_line_id
  ,plan_operation || ' ' || plan_options
  ,plan_object_name
ORDER BY plan_line_id
) qstats
,(
  SELECT count(*) AS totalsamples
  FROM v$active_session_history
  WHERE sql_id = :lastsql
  ) ns;



