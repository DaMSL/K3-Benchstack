FROM ubuntu:14.04
RUN apt-get update
RUN apt-get install -y wget default-jdk
RUN wget http://archive.cloudera.com/cdh5/one-click-install/trusty/amd64/cdh5-repository_1.0_all.deb
RUN dpkg -i cdh5-repository_1.0_all.deb
RUN apt-get update
RUN apt-get install -y zookeeper hadoop-yarn-resourcemanager hadoop-yarn-nodemanager hadoop-hdfs-datanode hadoop-mapreduce hadoop-client impala impala-shell
ADD master_startup.sh /sbin/
ADD slave_startup.sh /sbin/
RUN chmod +x /sbin/master_startup.sh
RUN chmod +x /sbin/slave_startup.sh

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/
