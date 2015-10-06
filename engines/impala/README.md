#Docker image:
Run all commands from within the 'docker' directory:
```
docker build -t damsl/impala .
docker push damsl/impala
```


#Ansible deployment:
Run all commands from within the 'deploy' directory:

####Configuration:
######hosts.ini:
An example is provided for an 8 node cluster at *deploy/hosts.ini*.

There should be two groups:
  - master: A Single machine that runs the Impala catalog and statestore
  - slaves: A List of machines to run the Impala daemon

######Other files:
See *deploy/files/* for other configuration files, to enable HDFS short circuit reads, and tweak other Impala configuration.

####Info:
Impala Web UI: http://*master_url*:25000

#Benchmarks
Run all commands from within the 'benchmark' directory. The *docker* command must be available.

#### Create and Load Tables (Phase 1)
```
ruby impala.rb -1
```

#### Run Benchmarks (Phase 2)
```
ruby impala.rb -2
```

For additional options, run:
```
ruby impala.rb --help
```
