FROM ubuntu:14.04
RUN apt-get update
# Java
RUN apt-get install -y wget default-jdk

# Flink
WORKDIR /software
RUN wget http://apache.cs.utah.edu/flink/flink-0.9.1/flink-0.9.1-bin-hadoop24.tgz
RUN tar -xvzf flink-0.9.1-bin-hadoop24.tgz
ADD jobmanager.sh /software/flink-0.9.1/bin/
ADD taskmanager.sh /software/flink-0.9.1/bin/

# Startup Scripts
ADD master_startup.sh /sbin/
ADD slave_startup.sh /sbin/
ADD flink_perf_start.sh /sbin/
ADD flink_perf_stop.sh /sbin/
RUN chmod +x /sbin/master_startup.sh \
             /sbin/slave_startup.sh \
             /sbin/flink_perf_start.sh \
             /sbin/flink_perf_stop.sh

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/
