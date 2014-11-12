FROM ubuntu:14.04
RUN apt-get update && apt-get install -y wget

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/

# Install Java
RUN sed 's/main$/main universe/' -i /etc/apt/sources.list
RUN apt-get update && apt-get install -y software-properties-common python-software-properties
RUN add-apt-repository ppa:webupd8team/java -y

RUN apt-get update
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

RUN apt-get install -y oracle-java7-installer
ENV JAVA_HOME /usr/lib/jvm/java-7-oracle/

# Install Hadoop
RUN wget http://apache.mirrors.tds.net/hadoop/common/hadoop-2.5.1/hadoop-2.5.1.tar.gz
RUN mkdir /software
RUN tar -C /software -xzf hadoop-2.5.1.tar.gz
ENV HADOOP_HOME /software/hadoop-2.5.1/
ENV HADOOP_COMMON_HOME /software/hadoop-2.5.1/
ENV HADOOP_HDFS_HOME /software/hadoop-2.5.1/
ENV HADOOP_MAPRED_HOME /software/hadoop-2.5.1/
ENV HADOOP_YARN_HOME /software/hadoop-2.5.1/

# Add startup scripts
ADD namenode_startup.sh /sbin/
ADD datanode_startup.sh /sbin/
RUN chmod +x /sbin/namenode_startup.sh
RUN chmod +x /sbin/datanode_startup.sh
