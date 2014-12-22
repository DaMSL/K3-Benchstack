# Running Oracle in a container

## Docker Exec
You can run docker exec to shell into the container. Ensure that the data is mounted (when you ran the docker run command). You may also want to mount a volume for all the scripts. 

## Remote Access
Accessing Oracle remotely requires SQL*Plus and loading data is provided by the SQL*Loader (sqlldr) program. Also, SQLPlus requires sudo access.


# LOADING DATA

## Data Naming
SQLLdr requires that all data files have a file extension (don't ask, it's a quirky Oracle-windows-thing). Thus, if your data files don't already have them, rename them all to a .dat file extension (default Oracle extention). You can use the rename command (WARNING: this command can be dangerous):

```
rename 's/$/.dat/' /local/data/tpch100g/customer/*
rename 's/$/.dat/' /local/data/tpch100g/lineitem/*
rename 's/$/.dat/' /local/data/tpch100g/nation/*
rename 's/$/.dat/' /local/data/tpch100g/orders/*
rename 's/$/.dat/' /local/data/tpch100g/partsupp/*
rename 's/$/.dat/' /local/data/tpch100g/region/*
rename 's/$/.dat/' /local/data/tpch100g/supplier/*
```

## Installing & configuring SQL*Loader:

FROM the Oracle 12c server: 
1. COPY  $ORACLE_HOME/bin/sqlldr TO client location (e.g. ~/oracle/cli/sqlldr).
2. COPY  $ORACLE_HOME/rdbms/mesg/  TO the client location (e.g. ~/oracle/cli/rdbms/mesg/). 

set LD_LIBRARY_PATH={local Oracle client lib dir} or in run context:

```

LD_LIBRARY_PATH=. ./sqlldr system/manager@localhost:1521/orcl control={control.ctl}
```

## Loading & Running
The following assumes that cwd is oracle/scripts/ and you have installed both SQLPlus and SQLLoader into oracle/cli. It also assumes scripts run on localhost ORCL_NET Listener bound to host port 11521 (that is, docker run was executed with '-p 11521:1521). This is designed to run on the TPC-H data set.

1. Make the loader control files:
```
./make_ctl_files.sh {PATH/TO/DATA}
```

2. Create the tables (adjust in-memory settings on these, as needed):
```
sudo LD_LIBRARY_PATH=../cli ../cli/sqlplus system/manager@localhost:11521/orcl <<< @tables.sql
```

3. Load all the data:
```
./load_data.sh
```

4. Run the queries
```
./run_queries.sh
```

Or, to run a single query you can either Docker exec in, or run one of the following:
```
sudo LD_LIBRARY_PATH=../cli ../cli/sqlplus system/manager@localhost:11521/orcl <<< @MY_QUERY.sql  {run once}

sudo LD_LIBRARY_PATH=../cli ../cli/sqlplus system/manager@localhost:11521/orcl  {enter SQL> interface}
```
