import subprocess
import os
import datetime
import sys
import psycopg2

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



def runVertica(args, conn):
  args = {}
  args['query_sql_dir'] = "../workloads/tpch/common/sql/queries"
  args['query_list_file'] = "../workloads/tpch/vertica/queries.txt"
  args['result_dir'] = "./vertica_results/"
  args['driver'] = "../workloads/vertica_driver.sh"
  args['schema'] = "tpch10g"
  args['trials'] = 1

  query_list = []
  with open(args['query_list_file'], "r") as f:
    # remove .sql extension and newline
    query_list = [ l[:-5] for l in f.readlines() ]

  command = "%s %s %s %s %s" % (args['driver'], args['query_sql_dir'], args['query_list_file'], args['schema'], args['trials'])
  subprocess.check_call(command, shell=True)

  trials = []
  ts = datetime.datetime.now()
  for q in query_list:
    path = os.path.join(args['result_dir'], q + ".sql")
    with open(path, "r") as f:
      i = 1
      for line in f.readlines():
        elapsed = line.split(" ")[-2]
        trial = Trial("Vertica", q, args['schema'], elapsed, i, ts)
        insertTrial(conn, trial)
        trials.append(trial)
        i = i + 1

  return trials 

if __name__ == "__main__":
  conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1")
  dropTables(conn)
  createTables(conn)
  runVertica({}, conn)
