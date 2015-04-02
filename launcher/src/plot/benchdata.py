import db.db as db

Oracle_comprmem = {'tpch10g':  {1:3.55, 3:5.13, 5:5.15, 6:3.55, 11:1.56, 18:5.13, 22:1.58}, 
                   'tpch100g': {1:38.19, 3:53.82, 5:53.97, 6:38.19, 11:11.87, 18:53.82, 22:15.63},
                   'amplab':   {1:5.6, 2:5.25, 3:10.85} }

#---------------------------------------------------------------------------------
#  Queries to retieve operational metrics
#--------------------------------------------------------------------------------
def mem_query (ds, sys):
  query = '''
SELECT E.query, avg(max_mem) AS memory, COALESCE(stddev(max_mem), 0) AS error
FROM trials T,
 (SELECT experiment_id, system, query FROM most_recent_by_system M WHERE M.dataset = '%s') E,
  (SELECT trial_id, sum(time) AS time, sum(percent_time) AS percent_time, sum(memory) AS sum_mem, max(memory) AS max_mem FROM operator_stats GROUP BY trial_id) O
  WHERE E.experiment_id = T.experiment_id AND T.trial_id = O.trial_id AND T.system = '%s' 
  GROUP BY E.experiment_id, E.query
UNION
SELECT query, memory, error
FROM k3memory
WHERE dataset='%s' AND system='%s';
''' % (ds, sys, ds, sys)
  return query

def time_query (ds, sys):
    query = "SELECT query, avg_time, error from summary_by_system where dataset='%s' and system='%s';" % (ds, sys)
    return query


def ipc_query (ds, sys):
    query = "SELECT query, ipc, 0 as error FROM k3cache where dataset='%s' AND system='%s';" % (ds, sys)
    return query

def cache_l2_query (ds, sys):
    query = "SELECT query, cache_l2, 0 as error FROM k3cache where dataset='%s' AND system='%s';" % (ds, sys)
    return query

def cache_l3_query (ds, sys):
    query = "SELECT query, cache_l3, 0 as error FROM k3cache where dataset='%s' AND system='%s';" % (ds, sys)
    return query




#---------------------------------------------------------------------------------
#  getOperationStats -- Returns mem, percent, time per op as matrix of values for each system
#--------------------------------------------------------------------------------
def getOperationStats(ds, qry, systems, operations): 
  memory = [[0. for s in systems] for o in operations]
  percent_time = [[0. for s in systems] for o in operations]
  abs_time = [[0. for s in systems] for o in operations]
  err_time = [0.] * len(systems)
  total_time = {}

  # Get avg time per system based on recorded ELAPSED time for given query
  conn = db.getConnection()
  cur = conn.cursor()
  query = "SELECT system, avg_time/1000, error/1000 from summary_by_system WHERE dataset='%s' and query='%s';" % (ds, qry)
  try:
    cur.execute(query)
    for result in cur.fetchall():
      sys, time, err = result
      if sys not in systems:
          continue
      total_time[sys] = float(time)
      err_time[systems.index(sys)] = float(err)
  except Exception as (ex):
    print (ex)
    print "Error processing the following query: \n\n%s\n" % query
    sys.exit(0)


  # Get percentage & memory data for each operation for each system for given query
  query = "SELECT S.system, op_name, avg(percent_time) as percent_time, avg(memory) as memory FROM operator_stats O, trials T, summary_by_system S WHERE S.experiment_id = T.experiment_id AND S.system = T.system AND O.trial_id = T.trial_id  AND S.dataset='%s' and S.query='%s' GROUP BY S.system, op_name;" % (ds, qry)

  try:
    cur.execute(query)
    for row in cur.fetchall():
      sys, op, percent, mem = row
      if sys not in systems:
          continue
      i, j = (operations.index(op.strip()), systems.index(sys))
      memory[i][j] = float(mem) / 1024.
      percent_time[i][j] = float(percent)
      if sys == 'Spark':
        percent_time[i][j] *= 100.
      
      #Calculate absolute time per operation based on measured percent time per operation out of reported elapsed time
      abs_time[i][j] = (percent_time[i][j] / 100.0) * total_time[sys]
  except Exception as (ex):
    print (ex)
    print "Error processing the following query: \n\n%s\n" % query
    sys.exit(0)

  # Manually Add in the Oracle im-memory tables
  if 'Oracle' in systems:
    oracle_memory = Oracle_comprmem[ds]
    memory[operations.index('TableScan')][systems.index('Oracle')] += oracle_memory[int(qry)] 

  if 'K3' in systems:
    query = "SELECT mem FROM something"
#    cur.execute(query)
#    result = sum ([float(r[0]) for r in cur.fetchall])
#    k3_memory = result
#    mem[operations.index('GroupBy')][system.index('K3')] = 

  return memory, percent_time, abs_time, err_time


#---------------------------------------------------------------------------------
#  getCadvisorMetrics -- Returns time-series of all mem & cpu data for all 
#      queries on all systems for given dataset
#--------------------------------------------------------------------------------
def getCadvisorMetrics(ds, systems, query_list):
  con = db.getConnection()
  cpu_data = {qry: {sys: [] for sys in systems} for qry in query_list}
  mem_data = {qry: {sys: [] for sys in systems} for qry in query_list}

  query = "SELECT S.query, C.system, C.cpu_usage_total, C.memory_usage FROM cadvisor_experiment_stats C, summary_by_system S WHERE S.dataset='%s' AND  C.system = S.system AND C.experiment_id = S.experiment_id ORDER BY query, system, interval;" % (ds)
  cur = con.cursor()
  cur.execute(query)
  for row in cur.fetchall():
      qry, sys, cpu, mem = row
      if sys not in systems:
          continue
      cpu_data[int(qry)][sys].append(cpu / 1000000)
      mem_data[int(qry)][sys].append(mem / 1024/1024/1024)
  return cpu_data, mem_data
