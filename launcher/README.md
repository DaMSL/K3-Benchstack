# Launcher
##Docker image:

The 'launcher' docker image contains all the necessary client-side software to run queries on the following systems:
  - Spark
  - Impala
  - Oracle
  - Vertica

See the Dockerfile for details. 

## Pre-requirements:
We assume that each system is already up and running with access to the required datasets.

The configuration file 'env.sh' specifies connection parameters for each of the systems.

##Starting a container:

To start a container for launching experiments run:
```
docker run -t -i --net=host damsl2:5020/damsl/launcher bash
```

Upon startup, the container will pull the K3-Benchstack repo, load the configuration from env.sh, and start up a webserver in the launcher/web folder for displaying plots.

##Running an experiment on a single system:

Each system provides a shell script for executing a single experiment on a single dataset, then reporting the elapsed time.

The scripts are located at launcher/src/systems/*<system>*/run\_*<system>*.sh

The scripts generally take a query file and a dataset name as arguments, but see usage for exact details. All necessary files will be located within the system's diectory.

##Automated experiments:
See the src/ folder.

The python program *launch.py* is the main driver.

Run:
```
python launch.py --help
```
for exact usage.

The arguments are:
  - --systems: A list of systems to run the exeriments on.
  - --trials: The number of trials for each experiment.
  - --tpch10g: A list of queries to run on the TPCH10g dataset.
  - --tpch100g: A list of queries to run on the TPCH100g dataset.

For example:
```
python launch.py --systems Spark Impala --trials 2 --tpch100g 1 3 5 6
```
## Results:
Results are stored in a postgres database running outside of the container.
More details to come.
