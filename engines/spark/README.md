#Docker image:
Run all commands from within the 'docker' directory:
```
docker build -t damsl/spark .
docker push damsl/spark
```

#Ansible deployment:
Run all commands from within the 'deploy' directory:

####Configuration:
######hosts.ini:
An example is provided for an 8 node cluster at *deploy/hosts.ini*.

There should be two groups:
  - master: A single Spark Master
  - workers: A list of Spark Workers

####Launch:
```
ansible-playbook -i hosts.ini plays/deploy_spark.yml
```

####Teardown:
```
ansible-playbook -i hosts.ini plays/teardown_spark.yml
```

####Info:
Spark Web UI: http://*master_url*:8081

Submit Spark jobs to the master at spark://*master_url*:7077

#Benchmarks
Run all commands from within the 'benchmark' directory. The *docker* command must be available.

#### Build JAR (Phase 1)
```
ruby spark.rb -1
```

#### Run Benchmarks (Phase 2)
```
ruby spark.rb -2
```

For additional options, run:
```
ruby spark.rb --help
```
