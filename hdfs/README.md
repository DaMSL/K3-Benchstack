#Deploy a HDFS cluster using a combination of Docker and Ansible:
##Docker image:

The docker folder contains a Dockerfile and some configuration files for building a simple HDFS image.
The image contains Java, Hadoop, and a few helpful scripts for startup.

It is currently hosted on the Docker registry at damsl/hdfs. 

You shouldn't need to edit any files in this folder, unless you need to make changes to the image.

##Ansible deployment:

The deploy folder contains files for deploying the HDFS cluster via Ansible.

To delpoy an HDFS cluster, you must first create a hosts file that lays out the topology of the cluster.

An example is provided for an 8 node cluster on our local machines.

There should be two groups: 
  - namenode:  Single machine (for now) that should run the HDFS namenode
  - datanodes: List of multiple machines that should run an HDFS datanode.

Be sure to use fully qualified domain names.

Additionally, specify a folder for persistent storage on the target machines.
All hdfs data and meta-data will be stored in this folder, and will persist even when the Docker cluster is torn down.

To launch the cluster, run the following command inside the 'deploy' folder:
```
ansible-playbook -i hosts.ini plays/deploy_hdfs.yml
```
You should be able to view the HDFS web interface at http://<namenode>:50070
You can issue HDFS commands by pointing the HDFS client at hdfs://<namenode>:54310
