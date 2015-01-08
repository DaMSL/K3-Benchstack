# Postgres (For statistics collection)
export PGHOST=mddb
export PGUSER=postgres
export PGPASSWORD=password

# Vertica
export VSQL_HOST=mddb
export VSQL_USER=dbadmin
export VSQL_PORT=5433
export VSQL_DATABASE=dbadmin

# Impala
export IMPALA_HOST=qp-hm1.damsl.cs.jhu.edu

# Oracle
# For now, we use mddb for tpch100g, and mddb2 for tpch10g
# TODO use mddb only. with a different database for tpch10g and tpch100g.
#export ORACLE_HOST=mddb2
export ORACLE_PORT=11521

# Spark
export SPARK_HOST=qp-hm1.damsl.cs.jhu.edu
export SPARK_PORT=7077
