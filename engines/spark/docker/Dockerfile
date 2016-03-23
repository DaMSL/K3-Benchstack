FROM debian:sid

# Install Oracle Java
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list
RUN echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN apt-get update
RUN apt-get install -y oracle-java8-installer oracle-java8-set-default

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/

# Install base software
RUN apt-get update && apt-get install -y \
      sudo curl bzip2 unzip autoconf Libtool \
      software-properties-common python-software-properties \
      gcc g++ make cmake git curl wget sudo \
      libyaml-cpp-dev libunwind-dev \
      subversion

# Install perf for kernel 3.13
# Also uses libunwind
# RUN apt-get install -y wget flex bison libelf-dev libaudit-dev libdw-dev binutils-dev && \
#   wget https://github.com/torvalds/linux/archive/v3.13.tar.gz && \
#   tar -xvzf v3.13.tar.gz && rm v3.13.tar.gz && \
#   cd linux-3.13/tools/perf && make && make install && \
#   cp perf /usr/bin && \
#   cp libperf.a /usr/lib && \
#   cd / && rm -Rf /linux-3.13

# Install Scala
RUN mkdir /software
WORKDIR /software
RUN curl -O http://www.scala-lang.org/files/archive/scala-2.10.4.tgz && \
    tar -xvzf scala-2.10.4.tgz
ENV SCALA_HOME /software/scala-2.10.4/bin/

# Install Spark
RUN apt-get install -y git && \
    curl -OL http://www.apache.org/dyn/closer.lua/spark/spark-1.6.1/spark-1.6.1.tgz && \
    tar -xvzf spark-1.6.1.tgz
ADD sbt-launch-lib.bash /software/spark-1.6.1/sbt/
WORKDIR /software/spark-1.6.1
RUN sbt/sbt -Dhadoop.version=2.2.0 -Pyarn -Phive assembly/assembly

WORKDIR /software
ENV SPARK_HOME /software/spark-1.6.1/

# Setup sbt
RUN curl -OL https://dl.bintray.com/sbt/native-packages/sbt/0.13.7/sbt-0.13.7.tgz
RUN tar -xvzf sbt-0.13.7.tgz
RUN mv sbt/bin/sbt* /usr/bin
RUN chmod +x /usr/bin/sbt
RUN sbt

ADD spark_master.sh /sbin/
ADD spark_worker.sh /sbin/
ADD spark_perf_start.sh /sbin/
ADD spark_perf_stop.sh /sbin/
ADD assets/bin/perfj /usr/bin/
ADD assets/lib/perfj-1.0.jar /usr/
RUN chmod +x /sbin/spark_master.sh /sbin/spark_worker.sh /sbin/spark_perf_start.sh /sbin/spark_perf_stop.sh /usr/bin/perfj

# Finish & Clean up
RUN ln -s /usr/lib/libmesos-0.22.1.so /usr/lib/libmesos.so && \
    apt-get remove -y gcc g++ make cmake git \
    software-properties-common \
    python-software-properties \
    unzip autoconf Libtool && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /usr/share/doc/* && \
    rm -rf /usr/share/man/* && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /var/lib/dpkg/* && \
    rm -rf /var/cache/* && \
    rm -rf /var/log/* && \
    rm -rf /usr/include/*

