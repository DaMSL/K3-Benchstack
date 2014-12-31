import psycopg2
import datetime 
from data import *
import plot
     
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
  experiments = []
  for i in [5]:
  #for i in [1, 3, 5, 6, 11, 18, 22]:
    experiments.append(Experiment("tpch",str(i),"tpch10g"))

  systems = [Impala.Impala(), Vertica.Vertica()]

  print("Preparing runners")
  runners = []
  for experiment in experiments:
    for system in systems:
      runner = system.getRunner(experiment)
      runners.append(runner)

  print("Running experiments")
  for runner in runners:
    print("Running  %s on %s" % (runner.experiment.name(), runner.system))
    result = runner.run()
    trial = None
    
    if isinstance(result, Failure):
      trial = Trial(runner.system, runner.experiment.query, runner.experiment.dataset, 0, 1, datetime.datetime.now())  
      print("Trial Failed:" + result.message)

    else:
      trial = Trial(runner.system, runner.experiment.query, runner.experiment.dataset, result, 1, datetime.datetime.now())
    insertTrial(conn, trial)
  
  plot.plotLatest(conn)
  conn.close()

