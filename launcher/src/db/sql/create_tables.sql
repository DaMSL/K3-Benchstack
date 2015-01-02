CREATE TABLE IF NOT EXISTS trials (
  run_id        serial primary key,
  system        text,
  query         text,
  dataset       text,
  trial         integer,
  ts            timestamp 
);

CREATE TABLE IF NOT EXISTS results (
  run_id        int,
  status        text,
  elapsed_ms    double precision
);


CREATE TABLE IF NOT EXISTS cadvisor (
  run_id	int,
  machine       text,
  timestamp text unique, 
  memory_usage bigint, 
  memory_working_set bigint,
  cpu_usage_system bigint, 
  cpu_usage_total bigint, 
  cpu_usage_user bigint, 
  network_rx_bytes bigint, 
  network_tx_bytes bigint
);

-- Find the most recent results per (system, query, dataset) for successful results
DROP VIEW IF EXISTS latest_results;
CREATE VIEW latest_results AS
   SELECT T1.system, T1.query, T1.dataset, T2.run_id, T2.elapsed_ms
   FROM
    (SELECT system, query, dataset, max(t.run_id) as max_run_id 
    FROM trials t, results r
    WHERE status = 'Success' or status ='Skipped' 
    group by system, query, dataset) as T1,
    results as T2
  WHERE T1.max_run_id = T2.run_id;

-- DROP VIEW IF EXISTS latest_trials_stats;
-- CREATE VIEW latest_trials_stats AS
  -- SELECT system, dataset, query, avg(elapsed_ms) as avg, coalesce(stddev(elapsed_ms),0) as error 
  -- FROM latest_trials as T, results as R
  -- WHERE T.run_id = R.run_id
  -- GROUP BY system, dataset, query;
