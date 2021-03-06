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
  network_tx_bytes bigint,
  interval int
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


CREATE TABLE IF NOT EXISTS llexperiments (
  llexp_id      serial primary key,
  llexp_name    text
);

CREATE TABLE IF NOT EXISTS lltrials (
  lltrial_id      serial primary key,
  llexp_id        integer,
  variant         text,
  query           text
);

CREATE TABLE IF NOT EXISTS lltime (
  lltrial_id      integer,
  elapsed         double precision
);

CREATE TABLE IF NOT EXISTS llheap (
  lltrial_id      integer,
  alloc           double precision
);

CREATE TABLE IF NOT EXISTS llcache (
  lltrial_id      integer,
  cache_L2        double precision,
  cache_L3        double precision,
  ipc             double precision,
  qpi             double precision
);



CREATE TABLE IF NOT EXISTS k3cache (
  system  text,
  dataset text,
  query   text,
  cache_L2        double precision,
  cache_L3        double precision,
  ipc             double precision,
  qpi             double precision);


CREATE TABLE IF NOT EXISTS k3memory (
  system  text,
  dataset text,
  query   text,
  memory  double precision,
  error   double precision DEFAULT 0.0);


DROP VIEW IF EXISTS llresults CASCADE;
CREATE VIEW llresults AS
SELECT E.llexp_name
  , T.variant
  , T.query
  , avg(R.elapsed) AS Elapsed
  , avg(alloc) as Heap
  , avg(cache_l2) as Cache_L2
  , avg(cache_l3) as Cache_L3
  , avg(ipc) as IPC
  , avg(qpi) as QPI
FROM 
  lltrials AS T
  , llexperiments AS E
  , (SELECT lltrial_id, elapsed, null as alloc, null::double precision as cache_L2, null::double precision as cache_L3, null::double precision as ipc, null::double precision as qpi from lltime
    UNION
    SELECT lltrial_id, null as elapsed, alloc, null as cache_L2, null as cache_L3, null as ipc, null as qpi from llheap
    UNION
    SELECT lltrial_id, null as elapsed, null as alloc, cache_L2, cache_L3, ipc, qpi from llcache
  ) AS R
WHERE 
  E.llexp_id = T.llexp_id 
  AND T.lltrial_id = R.lltrial_id
GROUP BY
  E.llexp_name, T.variant, T.query;






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
  WHERE trial_num > 2
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




DROP VIEW IF EXISTS most_recent_by_system;
CREATE VIEW most_recent_by_system AS
(SELECT workload, dataset, query, system, max(experiment_id) as experiment_id, max(trial_id) as trial_id
 FROM experiments NATURAL JOIN trials
 GROUP BY workload, dataset, query, system);


DROP VIEW IF EXISTS summary_by_system;
CREATE VIEW summary_by_system AS
 (SELECT m.experiment_id,
    m.workload,
    m.query,
    m.dataset,
    e.system,
    e.avg_time,
    e.error
  FROM most_recent_by_system M
    JOIN experiment_stats E 
    USING (workload, query, dataset, experiment_id, system) );






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


DROP VIEW IF EXISTS cadvisor_baseline_stats CASCADE;
CREATE VIEW cadvisor_baseline_stats AS (
	 SELECT c.trial_id,
	    c.machine,
	    c.interval,
	    c.cpu_usage_total - b.cpu_usage_total AS cpu_usage_total,
	    c.cpu_usage_system - b.cpu_usage_system AS cpu_usage_system,
	    c.cpu_usage_user - b.cpu_usage_user AS cpu_usage_user,
	    c.network_rx_bytes - b.network_rx_bytes AS network_rx_bytes,
	    c.network_tx_bytes - b.network_tx_bytes AS network_tx_bytes,
	    c.memory_usage,
	    c.memory_working_set
	   FROM ( SELECT trial_id,
	            machine,
	            interval,
	            cpu_usage_total,
	            cpu_usage_system,
       			cpu_usage_user,
	            network_rx_bytes,
           	 	network_tx_bytes
	           FROM cadvisor
		          WHERE cadvisor.interval = 1) B,
		    cadvisor c
		  WHERE c.trial_id = b.trial_id AND c.machine = b.machine);

DROP VIEW IF EXISTS cadvisor_trial_stats CASCADE;
CREATE VIEW cadvisor_trial_stats AS (
	 SELECT trial_id,
	    interval,
	    sum(memory_usage) AS memory_usage,
	    sum(memory_working_set) AS memory_working_set,
	    avg(cpu_usage_total) AS cpu_usage_total,
	    avg(cpu_usage_system) AS cpu_usage_system,
	    avg(cpu_usage_user) AS cpu_usage_user,
	    sum(network_rx_bytes) AS network_rx_bytes,
	    sum(network_tx_bytes) AS network_tx_bytes
	   FROM cadvisor_baseline_stats
	  GROUP BY trial_id, interval); 
  
  
DROP VIEW IF EXISTS cadvisor_experiment_stats CASCADE;
CREATE VIEW cadvisor_experiment_stats AS (
	 SELECT experiment_id,
	    system,
	    interval,
	    count(trial_id) AS num_samples,
	    avg(cpu_usage_total) AS cpu_usage_total,
	    avg(memory_usage) AS memory_usage
	   FROM cadvisor_trial_stats
	     JOIN trials USING (trial_id)
	  WHERE trials.trial_num > 2
			  GROUP BY experiment_id, system, interval);



DROP VIEW IF EXISTS cadvisor_collected CASCADE;
CREATE VIEW cadvisor_collected AS (  
SELECT 
  trial_id
  , interval
  , sum(memory_usage) as memory_usage
  , sum(memory_working_set) as memory_working_set
  , avg(cpu_usage_total) as cpu_usage_total
  , avg(cpu_usage_system) as cpu_usage_system
  , avg(cpu_usage_user) as cpu_usage_user
  , sum(network_rx_bytes) as network_rx_bytes
  , sum(network_tx_bytes) as network_tx_bytes
FROM 
  cadvisor 
GROUP BY
  trial_id
  , interval);

  
DROP VIEW IF EXISTS cadvisor_summary CASCADE;
CREATE VIEW cadvisor_summary AS (  
SELECT 
  experiment_id
  , system
  , interval
  , avg(memory_usage) as memory_usage
  , avg(memory_working_set) as memory_working_set
  , avg(cpu_usage_total) as cpu_usage_total
  , avg(cpu_usage_system) as cpu_usage_system
  , avg(cpu_usage_user) as cpu_usage_user
  , avg(network_rx_bytes) as network_rx_bytes
  , avg(network_tx_bytes) as network_tx_bytes
FROM 
  cadvisor_collected NATURAL JOIN trials 
GROUP BY
  experiment_id
  , system
  , interval);


DROP VIEW IF EXISTS cadvisor_aggregated CASCADE;
CREATE VIEW cadvisor_aggregated AS (
	SELECT 
	  experiment_id
	  , system
	  , interval
	  , count(trial_id) trials
	  , avg(cpu_usage_total) as cpu_usage_total
	  , avg(memory_usage) as memory_usage
	FROM 
	  cadvisor_collected natural join trials 
	WHERE 
	  trial_num > 2 
	GROUP BY
	  experiment_id, system, interval);



DROP VIEW IF EXISTS cadvisor_baselined CASCADE;
CREATE VIEW cadvisor_baselined AS
(SELECT  
	  C.experiment_id
	  , C.system
	  , interval
	  , trials
	  , (C.cpu_usage_total - B.base) as cpu_usage_total
	  , C.memory_usage
	FROM 
	  (select system, experiment_id, cpu_usage_total as base from cadvisor_aggregated where interval = 1) as B
	  , cadvisor_aggregated AS C
	WHERE 
	  C.experiment_id = B.experiment_id 
	  AND C.system = B.system);


drop view if exists mostRecentK3;
create view mostRecentK3 as
select max(experiment_id) as experiment_id, workload, dataset, query from experiments natural join trials where system='K3' group by workload, dataset, query order by workload, dataset, query;

drop view if exists mostRecentK3Results;
create view mostRecentK3Results as
select experiment_id, trial_id, workload, query, dataset, elapsed_ms, ts from mostRecentK3 natural join trials natural join results order by workload, query ,dataset, trial_id;

drop view if exists mostRecentK3Averages;
create view mostRecentK3Averages as
select experiment_id, workload, query, dataset, avg(elapsed_ms) as avg_time, stddev(elapsed_ms) as stddev_time, stddev(elapsed_ms)/avg(elapsed_ms) as stddev_avg_ratio, max(ts) as ts from mostRecentK3Results group by experiment_id, workload, query, dataset order by workload, dataset, query;

drop view if exists mostRecentK3Scalability;
create view mostRecentK3Scalability as
select *, avg_time / cast(dataset as int) as time_per_core from mostRecentK3Averages where workload ='scalability' order by cast(query as int), cast(dataset as int);

drop view if exists mostRecentK3MLScalability;
create view mostRecentK3MLScalability as
select *, avg_time / cast(dataset as int) as time_per_core from mostRecentK3Averages where workload ='ml_scalability' order by cast(query as int), cast(dataset as int);

drop view if exists highDeiations;
create view highDeviations as
select * from mostRecentK3Averages where stddev_avg_ratio > .1;
