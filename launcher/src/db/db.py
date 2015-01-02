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

# Insert a trial and return the run_id associated with it.
def insertTrial(conn, trial):
  try:
    query = "INSERT INTO trials (system, query, dataset, trial, ts) VALUES (%s, %s, %s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, trial.tup())
    conn.commit()

    query = "SELECT MAX(run_id) FROM trials;"
    cur.execute(query)
    return int(cur.fetchone()[0])

  except Exception as inst:
      print("Failed to insert Trial: ")
      print(inst)
      sys.exit(1)

def insertResult(conn, result):
  try:
    query = "INSERT INTO results (run_id, status, elapsed_ms) VALUES (%s, %s, %s)"
    cur = conn.cursor()
    cur.execute(query, result.tup())
    conn.commit()
  except Exception as inst:
      print("Failed to insert Trial: ")
      print(inst)
      sys.exit(1)
