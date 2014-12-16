#Zookeeper
##Configuring

Zookeeper config is set up to run in standalone mode. Just docker pull (or build) & run the container -- be sure to expose port 2181.

To configure for replication mode, you need to update the zoo.cfg for all servers and set the myid for EACH server separately (you shouldn't more than 3 or 5, so it's a simple task). It is important that the ID matches the config:

FIRST:
In zoo.cfg, update the zookeeper server list to the set of servers you want to run. The provided zoo.cfg example defines qp1, qp2, and qp3 as servers 1, 2, & 3 respectively. Then on EACH host machine, simply "echo $id > myid." 

THEN: 
Build the image with the Dockerfile and run it. You can start it in shell with the command:

bin/zkServer.sh start

The entry point should be the default ZK location (/opt/zookeeper-3.4.5). 


##Using Zookeeper

Be sure to expose port 2181, 2888, 3888 (or run on host networking).

Tips to verify (in replication mode):

"echo ruok | nc <host> 2181"

Should simply return "imok"


"echo stat | nc <host> 2181"

Should return current ZK information (follower/leader, connections, etc...).


You can also run a shell into the container and run the client for more granular setting/capabilities and to manage zNodes via 'bin/zkCli.sh'
