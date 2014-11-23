#Deploy an Impala cluster using a combination of Docker and Ansible:
##Docker image:

The docker folder contains a Dockerfile and some configuration files for building a simple Impala image.
The image contains Java, Impala, and related services.

It is currently hosted on the Docker registry at damsl/impala.

You shouldn't need to edit any files in this folder, unless you need to make changes to the image.

##Ansible deployment:

The deploy folder contains files for deploying the Impala cluster via Ansible.

To deploy a Impala cluster, you must first create a hosts file that lays out the topology of the cluster.

An example is provided for an 8 node cluster on our local machines.

There should be two groups: 
  - master:  Single machine (for now) that should run the Impala catalog and statestore.
  - slaves: List of multiple machines that should run an Impala daemon process.

Be sure to use fully qualified domain names.

To launch the cluster, run the following command inside the 'deploy' folder:
```
ansible-playbook -i hosts.ini plays/deploy_impala.yml
```
You should be able to view the Impalad web interface at http://*slave*:25000

Connect to the cluster by running impala-shell and executing:
```
connect *slave*;
```
