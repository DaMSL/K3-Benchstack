import psycopg2
import datetime 
import plot

class Experiment:
  # TODO validate that query and dataset are compatible with workload
  def __init__(self, workload, query, dataset):
    self.workload = workload
    self.query = query
    self.dataset = dataset

  def name(self):
    return ("workload: %s. query: %s. dataset: %s" % (self.workload, self.query, self.dataset))

class Failure:
  def __init__(self, message):
    self.message = message

class Trial:
  def __init__(self, system, query, dataset, elapsed, trial, ts):
    self.system = system
    self.query = query
    self.dataset = dataset
    self.trial = trial
    self.elapsed = elapsed
    self.ts = ts

  def tup(self):
    return (self.system, self.query, self.dataset, self.trial, self.elapsed, self.ts) 
     
def createTables(conn):
  with open("sql/create_tables.sql","r") as f:
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
  with open("sql/drop_tables.sql","r") as f:
    try:
      query = f.read()
      cur = conn.cursor()
      cur.execute(query)
      conn.commit()
    except Exception as inst:
      print("Failed to drop tables:" )
      print(inst)
      sys.exit(1)

def insertTrial(conn, trial):
  try:
    query = "INSERT INTO trials VALUES (%s, %s, %s, %s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, trial.tup())
    conn.commit()
  except Exception as inst:
      print("Failed to insert Trial: ")
      print(inst)
      sys.exit(1)

from impala import Impala
from vertica import Vertica

if __name__ == "__main__":
  print("Setting up Database")
  conn = psycopg2.connect("dbname=postgres")
  dropTables(conn)
  createTables(conn)

  print("Preparing Experiments")
  tpch1 = Experiment("tpch","1","tpch10g")
  tpch3 = Experiment("tpch","3","tpch10g")
  tpch6 = Experiment("tpch","6","tpch10g")
  tpch11 = Experiment("tpch","11","tpch10g")

  experiments = [tpch1, tpch3, tpch6, tpch11]

  systems = [Impala.Impala(), Vertica.Vertica()]

  print("Preparing runners")
  runners = []
  for experiment in experiments:
    for system in systems:
      runner = system.getRunner(experiment)
      runners.append(runner)

  print("Running experiments")
  for runner in runners:
    #print("Running  %s on %s" % (runner.experiment.name(), runner.system))
    result = runner.run()
    trial = None
    
    if isinstance(result, Failure):
      trial = Trial(runner.system, runner.experiment.query, runner.experiment.dataset, 0, 1, datetime.datetime.now())  
    else:
      trial = Trial(runner.system, runner.experiment.query, runner.experiment.dataset, result, 1, datetime.datetime.now())
    insertTrial(conn, trial)
  
  plot.plotLatest(conn)
  conn.close()

