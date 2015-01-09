import psycopg2
import sys

def getConnection():
  conn = psycopg2.connect("dbname=postgres password=password")
  return conn

def createTables(conn):
  with open("db/sql/create_tables.sql","r") as f:
    try:
      query = f.read()
      cur = conn.cursor()
      cur.execute(query)
      conn.commit()
    except Exception as inst:
      print("Failed to create tables:" )
      print(inst)
      sys.exit(1)

def dropTables(conn):
  with open("db/sql/drop_tables.sql","r") as f:
    try:
      query = f.read()
      cur = conn.cursor()
      cur.execute(query)
      conn.commit()
    except Exception as inst:
      print("Failed to drop tables:" )
      print(inst)
      sys.exit(1)

# Insert an experiment and return the experiment_id associated with it
def insertExperiment(conn, exp):
  try:
    query = "INSERT INTO experiments (workload, query, dataset) VALUES (%s, %s, %s) RETURNING experiment_id"
    cur = conn.cursor()
    cur.execute(query, exp.tup())
    val = int(cur.fetchone()[0])
    conn.commit()
    return val

  except Exception as inst:
      print("Failed to insert Experiment: ")
      print(inst)
      sys.exit(1)


# Insert a trial and return the trial_id associated with it.
def insertTrial(conn, trial):
  try:
    query = "INSERT INTO trials (experiment_id, trial_num, system, ts) VALUES (%s, %s, %s, %s) RETURNING trial_id"
    cur = conn.cursor()
    cur.execute(query, trial.tup())
    val = int(cur.fetchone()[0])
    conn.commit()
    return val

  except Exception as inst:
      print("Failed to insert Trial: ")
      print(inst)
      sys.exit(1)

def insertResult(conn, result):
  try:
    query = "INSERT INTO results (trial_id, status, elapsed_ms, notes) VALUES (%s, %s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, result.tup())
    conn.commit()
  except Exception as inst:
      print("Failed to insert Result: ")
      print(inst)
      sys.exit(1)

def insertOperator(conn, operator):
  try:
    query = "INSERT INTO operator_metrics VALUES (%s, %s, %s, %s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, operator.tup())
    conn.commit()
  except Exception as inst:
      print("Failed to insert Operator: ")
      print(inst)
      sys.exit(1)

def getPlotData(conn):
  try:
    query = "select * from experiment_stats e where not exists (select * from plots where experiment_id = e.experiment_id);"
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    return results
  except Exception as inst:
      print("Failed to get new plot data: ")
      print(inst)
      sys.exit(1)

def getMetricPlotData(conn):
  try:
    query = "SELECT T.experiment_id,trial_id, trial_num, system, query, dataset, workload FROM trials as T, experiments AS E WHERE T.experiment_id = E.experiment_id and NOT EXISTS (select * from metric_plots where trial_id = T.trial_id);"
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    return results
  except Exception as inst:
      print("Failed to get new metric plot data: ")
      print(inst)
      sys.exit(1)

def registerPlot(conn, exp_id):
  try:
    query = "INSERT INTO plots VALUES (%s);"
    cur = conn.cursor()
    cur.execute(query, (exp_id,) )
    conn.commit()
  except Exception as inst:
      print("Failed to insert plot: ")
      print(inst)
      sys.exit(1)

def registerMetricPlot(conn, trial_id):
  try:
    query = "INSERT INTO metric_plots VALUES (%s);"
    cur = conn.cursor()
    cur.execute(query, (trial_id,) )
    conn.commit()
  except Exception as inst:
      print("Failed to insert metric plot: ")
      print(inst)
      sys.exit(1)
