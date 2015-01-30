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
	  ,max(plan_depth) as depth
	  ,plan_operation || ' ' || plan_options
	  ,max(object) as object
	  ,count(ash.sql_plan_line_id) AS numsamples
	  ,max(mem)
	  ,max(pga.pga_allocated) as max_pga
	  ,coalesce(avg(ash.PGA_ALLOCATED), 0) as avg_pga
	FROM (
		select sql_id, plan_line_id, plan_depth, plan_operation, plan_options, max(plan_object_name) as object, sid, COALESCE(max(workarea_max_mem), 0) as mem from v$sql_plan_monitor where sql_id=:lastsql group by sql_id, plan_line_id, plan_depth, plan_operation, plan_options, sid  order by plan_line_id) pm
	  ,(SELECT SQL_plan_line_id
		    ,sql_plan_operation
		    ,sample_id
		    ,count(session_id) AS numsamples
		    ,coalesce(sum(PGA_ALLOCATED), 0) as pga_allocated
		    FROM 
		      v$active_session_history ash
		    WHERE sql_id=:lastsql
		    GROUP BY sql_plan_line_id,sql_plan_operation, sample_id
		    ORDER BY sql_plan_line_id, sample_id) pga
	  ,v$active_session_history ash
	WHERE pm.plan_line_id = ash.sql_plan_line_id(+)
	  AND pm.sql_id = ash.sql_id(+)
	  AND pm.sid = ash.session_id(+)
	  AND ash.sql_id=:lastsql
	  AND ash.sample_id = pga.sample_id
	  AND ash.sql_plan_line_id = pga.sql_plan_line_id
	GROUP BY plan_line_id
	  ,plan_operation || ' ' || plan_options
	ORDER BY plan_line_id
) qstats
,(
	  SELECT NULLIF(count(*), 0) AS totalsamples
	  FROM v$active_session_history
	  WHERE sql_id=:lastsql) ns;
