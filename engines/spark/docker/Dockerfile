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

# Install Scala
RUN mkdir /software
WORKDIR /software
RUN wget http://www.scala-lang.org/files/archive/scala-2.10.4.tgz
RUN tar -xvzf scala-2.10.4.tgz
ENV SCALA_HOME /software/scala-2.10.4/bin/

# Install Spark
RUN wget http://apache.mirrors.tds.net/spark/spark-1.1.0/spark-1.1.0-bin-hadoop2.4.tgz
RUN tar -xvzf spark-1.1.0-bin-hadoop2.4.tgz

# Setup sbt
RUN wget https://dl.bintray.com/sbt/native-packages/sbt/0.13.7/sbt-0.13.7.tgz
RUN tar -xvzf sbt-0.13.7.tgz
RUN mv sbt/bin/sbt-launch.jar /usr/bin/
ADD sbt /usr/bin/
RUN chmod +x /usr/bin/sbt
RUN sbt

ENV SPARK_HOME /software/spark-1.1.0-bin-hadoop2.4/
