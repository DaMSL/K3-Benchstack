create table Globals (
  _mess_id           int,
  _dest_peer         text,
  args               json,
  date_lb            varchar,
  date_ub            varchar,
  elapsed_ms         int,
  empty_aggs         json,
  empty_source_ip_to_aggs_C json,
  end_ms             int,
  flag               boolean,
  flag2              boolean,
  global_max         json,
  local_max          json,
  master             json,
  me                 json,
  merged_partials    json,
  merged_peers       int,
  outFile            varchar,
  partial_aggs       json,
  peer_seq           json,
  peers              json,
  peers_done         int,
  peers_ready        int,
  q3_result          json,
  rankingsMap        json,
  rkFiles            json,
  role               varchar,
  start_ms           int,
  user_visits        json,
  uvFiles            json
);

create table Messages (
  _mess_id           int,
  _dest_peer         text,
  trigger            text,
  source_peer        text,
  contents           text,
  time               text
);


create table Results (
  sourceIP           varchar,
  totalRevenue       double precision,
  avgPageRank        double precision
);

create table CorrectResults (
  sourceIP           varchar,
  totalRevenue       double precision,
  avgPageRank        double precision
);


create or replace function load_q3_result
(filename text)
returns void as $$
declare jsonVar json;
begin
drop table if exists temp_load_q3_result;
create temporary table temp_load_q3_result (logEntry json);
execute format('copy temp_load_q3_result from ''%s'' ', filename);
select logEntry into jsonVar FROM temp_load_q3_result limit 1;
insert into Results select (t#>>'{value,sourceIP}')::varchar,(t#>>'{value,totalRevenue}')::double precision,(t#>>'{value,avgPageRank}')::double precision from json_array_elements(jsonVar->'value') as R(t);
end;
$$ language plpgsql volatile;
create view compute_diff as
SELECT * FROM CorrectResults as l
WHERE NOT EXISTS
(SELECT * FROM Results as r
WHERE l.sourceIP = r.sourceIP and ABS(l.totalRevenue-r.totalRevenue) > .01 and ABS(l.avgPageRank-r.avgPageRank) > .01);


select
    load_q3_result('/tmp/k3_results/k3_q3_new/192.168.0.40/40000/192.168.0.40:40000_Result.txt');

select
    load_q3_result('/tmp/k3_results/k3_q3_new/192.168.0.40/40001/192.168.0.40:40001_Result.txt');




