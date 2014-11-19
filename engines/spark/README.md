#Deploy a Spark cluster using a combination of Docker and Ansible:
##Docker image:

The docker folder contains a Dockerfile and some configuration files for building a simple Spark image.
The image contains Java, Scala, and Spark.

It is currently hosted on the Docker registry at damsl/spark.

You shouldn't need to edit any files in this folder, unless you need to make changes to the image.

##Ansible deployment:

The deploy folder contains files for deploying the Spark cluster via Ansible.

To deploy a Spark cluster, you must first create a hosts file that lays out the topology of the cluster.

An example is provided for an 8 node cluster on our local machines.

There should be two groups: 
  - master:  Single machine (for now) that should run the Spark Master process.
  - workers: List of multiple machines that should run a Spark Worker process.

Be sure to use fully qualified domain names.

To launch the cluster, run the following command inside the 'deploy' folder:
```
ansible-playbook -i hosts.ini plays/deploy_spark.yml
```
You should be able to view the HDFS web interface at http://*master*:8080

You can submit Spark jobs to the master at spark://*master*:7077
