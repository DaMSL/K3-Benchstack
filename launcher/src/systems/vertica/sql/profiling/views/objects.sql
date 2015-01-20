DROP VIEW IF EXISTS objects;
CREATE VIEW objects AS
SELECT transaction_id, statement_id, path_id, group_concat(path_line) over (partition by transaction_id, statement_id, path_id) as description from query_plan_profiles;

DROP VIEW IF EXISTS ops;
CREATE VIEW ops AS
SELECT transaction_id, statement_id, path_id, group_concat(operator_name) over (partition by transaction_id, statement_id, path_id) as ops from importantStats;
