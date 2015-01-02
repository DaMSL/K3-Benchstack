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
  timestamp text unique, 
  memory_usage bigint, 
  memory_working_set bigint,
  cpu_usage_system bigint, 
  cpu_usage_total bigint, 
  cpu_usage_user bigint, 
  network_rx_bytes bigint, 
  network_tx_bytes bigint
);

-- DROP VIEW IF EXISTS latest_trials;
-- CREATE VIEW latest_trials AS
  -- SELECT  T2.* 
  -- FROM
     -- (SELECT system, query, dataset, max(ts) AS max_ts 
      -- FROM trials 
      -- GROUP BY system, query, dataset) as T1,
     -- trials as T2 
  -- WHERE T1.system = T2.system and T1.query = T2.query and T1.dataset = T2.dataset and T1.max_ts = T2.ts;

-- DROP VIEW IF EXISTS latest_trials_stats;
-- CREATE VIEW latest_trials_stats AS
  -- SELECT system, dataset, query, avg(elapsed_ms) as avg, coalesce(stddev(elapsed_ms),0) as error 
  -- FROM latest_trials as T, results as R
  -- WHERE T.run_id = R.run_id
  -- GROUP BY system, dataset, query;
