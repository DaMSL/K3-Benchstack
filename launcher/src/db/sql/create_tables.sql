CREATE TABLE IF NOT EXISTS experiments (
  experiment_id serial primary key,
  workload             text,
  query                text,
  dataset              text
);

CREATE TABLE IF NOT EXISTS trials (
  trial_id      serial primary key,
  experiment_id integer, 
  trial_num     integer,
  system        text,
  ts            timestamp 
);

CREATE TABLE IF NOT EXISTS results (
  trial_id      int,
  status        text,
  elapsed_ms    double precision,
  notes         text
);

CREATE TABLE IF NOT EXISTS cadvisor (
  trial_id	int,
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

CREATE TABLE IF NOT EXISTS operator_metrics (
  trial_id          int,
  operator_num      int,
  operator_name     text,
  time              double precision,
  percent_time      double precision,
  memory            double precision,
  object            text
);

CREATE TABLE IF NOT EXISTS plots (
  experiment_id     int
);

CREATE TABLE IF NOT EXISTS metric_plots (
  trial_id     int
);

-- Results of all trials
DROP VIEW IF EXISTS trial_results CASCADE;
CREATE VIEW trial_results AS
SELECT 
  experiment_id, t.trial_id, system, trial_num, status, elapsed_ms
FROM 
  trials t, results r
WHERE
      t.trial_id = r.trial_id  
  and r.status <> 'Failure'
ORDER BY 
  experiment_id, system, trial_num;

-- Statistics of each experiment
DROP VIEW IF EXISTS experiment_stats CASCADE;
CREATE VIEW experiment_stats AS
SELECT
  e.workload, e.dataset, e.query, T.*
FROM
  experiments e,
  (SELECT
    experiment_id, system, AVG(elapsed_ms) as avg_time, coalesce(stddev(elapsed_ms), 0) as error, count(*) as num_trials
  FROM
    trial_results
  GROUP BY
    experiment_id, system) T
WHERE
  e.experiment_id = T.experiment_id
ORDER BY
  T.experiment_id, T.system;

DROP VIEW IF EXISTS operator_plots CASCADE;
CREATE VIEW operator_plots AS
SELECT 
  experiment_id, system, operator_name, substring(object,0,20) as obj , operator_num, avg(percent_time) as percent_time 
FROM 
  trials 
natural join 
  experiments 
natural join 
  operator_metrics 
group by 
  experiment_id, system, operator_name, obj, operator_num 
order by experiment_id, system, operator_num;


DROP VIEW IF EXISTS most_recent;
create view most_recent as 
select workload, query, dataset, max(experiment_id) as experiment_id 
from experiments 
group by workload, dataset, query, dataset;

DROP VIEW IF EXISTS summary;
create view summary as
select experiment_id, workload,query,dataset,system, avg_time, error  from most_recent natural join experiment_stats;


CREATE TABLE IF NOT EXISTS operator_names (
  op_name   text,
  op_list   text
);


DROP VIEW IF EXISTS operator_stats CASCADE;
CREATE VIEW operator_stats AS
SELECT 
  trial_id, op_name, sum(time) as time, sum(percent_time) as percent_time, sum(memory) as memory 
FROM 
  operator_metrics as m, operator_names as n 
WHERE 
  m.operator_name = n.op_list 
GROUP BY
  trial_id, op_name;

