CREATE TABLE IF NOT EXISTS trials (
  system        text,
  query         text,
  dataset       text,
  trial         integer,
  elapsed_ms    double precision,
  ts            timestamp 
);

DROP VIEW IF EXISTS latest_trials;
CREATE VIEW latest_trials AS
  SELECT  T2.* 
  FROM
     (SELECT system, query, dataset, max(ts) AS max_ts 
      FROM trials 
      GROUP BY system, query, dataset) as T1,
     trials as T2 
  WHERE T1.system = T2.system and T1.query = T2.query and T1.dataset = T2.dataset and T1.max_ts = T2.ts;

DROP VIEW IF EXISTS latest_trials_stats;
CREATE VIEW latest_trials_stats AS
  SELECT system, dataset, query, avg(elapsed_ms) as avg, coalesce(stddev(elapsed_ms),0) as error 
  FROM latest_trials 
  GROUP BY system, dataset, query;
