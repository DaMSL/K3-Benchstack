

####################################################################
#
#  Dockerfile for ZooKeeper  -- updated for Damsl 
#
#####################################################################

FROM quay.io/signalfuse/maestro-base:14.04-0.1.8.1
MAINTAINER Maxime Petazzoni <max@signalfuse.com>

# Get stable release of ZooKeepera (3.4.5)
RUN wget -q -O - http://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz | tar -C /opt -xz

RUN mkdir /var/zookeeper

ADD myid /var/zookeeper/
ADD zoo.cfg /opt/zookeeper-3.4.5/conf/
ADD run_zookeeper.sh /opt/zookeeper-3.4.5/
#ADD run.py /opt/zookeeper-3.4.5/

WORKDIR /opt/zookeeper-3.4.5/

CMD ["./run_zookeeper.sh"]
