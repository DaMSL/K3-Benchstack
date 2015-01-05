import psycopg2
import sys

def getConnection():
  conn = psycopg2.connect("dbname=postgres")
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
    query = "INSERT INTO experiments (workload, query, dataset) VALUES (%s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, exp.tup())
    conn.commit()

    query = "SELECT MAX(experiment_id) FROM experiments;"
    cur.execute(query)
    return int(cur.fetchone()[0])

  except Exception as inst:
      print("Failed to insert Experiment: ")
      print(inst)
      sys.exit(1)


# Insert a trial and return the trial_id associated with it.
def insertTrial(conn, trial):
  try:
    query = "INSERT INTO trials (experiment_id, trial_num, system, ts) VALUES (%s, %s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, trial.tup())
    conn.commit()

    query = "SELECT MAX(trial_id) FROM trials;"
    cur.execute(query)
    return int(cur.fetchone()[0])

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
