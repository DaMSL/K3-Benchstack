#Zookeeper
##Installation

Zookeeper config is set up to run in cluster mode (vice single server). In order to run it, you need to update the zoo.cfg for all servers and set the myid for EACH server. It is important that the ID matches the config:

FIRST:
In zoo.cfg, update the zookeeper server list to the set of servers you want to run. In the provided zoo.cfg, Zk is currently set to run on qp1, qp2, and qp3 as servers 1, 2, & 3 respectively. Then on EACH host machine, simply "echo $id > myid." 

THEN: 
Build the image with the Dockerfile. The run.py is a python script to start up & run zookeeper. Alternatively, you can start it with the command:

bin/zkServer.sh

The entry point should be the default ZK location (/opt/zookeeper-3.4.5). 


##Running Zookeeper

Be sure to expose port 2181 (or run on host networking).

Tips to verify:

"echo ruok | nc <host> 2181"

Should simply return "imok"


"echo stat | nc <host> 2181"

Should return current ZK information (follower/leader, connections, etc...).


You can also run a shell into the container and run the client for more granular setting/capabilities and to manage zNodes via 'bin/zkCli.sh'
