select 
  SQL_PLAN_LINE_ID
  ,SQL_PLAN_OPERATION
  ,max(pga_allocated)
FROM 
    (Select
      SAMPLE_ID
      ,SQL_PLAN_LINE_ID
      ,SQL_PLAN_OPERATION
      ,sum(PGA_ALLOCATED) AS pga_allocated
    from 
    v$active_session_history
    where sql_id=:lastsql
    GROUP BY
      sample_id, sql_plan_line_id, sql_plan_operation)
GROUP BY
  SQL_PLAN_LINE_ID
  ,SQL_PLAN_OPERATION;
