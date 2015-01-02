import psycopg2

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
