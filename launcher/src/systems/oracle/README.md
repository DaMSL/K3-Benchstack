# ORACLE
##Loading Data:

Data is loaded into Oracle via the SQLLDR program. If run locally, it requires sudo privs. Alternatively, you can mount the local data volume into the Oracle container and run it locally. Control files are provided for loading data. You will need to update the files to provide the appropriate input dir for each source table. Wildcard chars can be used to load an entire directory of partitioned files. Ensure that the files are appened with a file extension (.dat is the standard). SQLLDR will not accept input files without an extension (weird quirk).

Command to load is:
```
sqlldr system/manager@mddb:11521/orcl control=partsupp.ctl
```

## Running Queries:
The run_oracle.sh script will prepare and run the query for benchmarking. It turns off feedback, forces result cache flushing, tags the  query for metric retrieval and then adds in the additional metric retrieval queries from getlast.sql. The getlast.sql queries the internal database after execution to retrieve time and mem usage data per operation.

