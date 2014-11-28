FROM ubuntu:14.04
RUN apt-get update
# Java
RUN apt-get install -y wget default-jdk

# Flink
WORKDIR /software
RUN wget http://mirror.reverse.net/pub/apache/incubator/flink/flink-0.7.0-incubating/flink-0.7.0-incubating-bin-hadoop2.tgz
RUN tar -xvzf flink-0.7.0-incubating-bin-hadoop2.tgz
ADD jobmanager.sh /software/flink-0.7.0-incubating/bin/
ADD taskmanager.sh /software/flink-0.7.0-incubating/bin/

# Startup Scripts
ADD master_startup.sh /sbin/
ADD slave_startup.sh /sbin/
RUN chmod +x /sbin/master_startup.sh
RUN chmod +x /sbin/slave_startup.sh

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/
