FROM debian:sid

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get update && apt-get install -y \
  bzip2 unzip autoconf Libtool \
  gcc g++ openjdk-8-jdk make cmake git curl wget sudo \
  libyaml-cpp-dev libunwind-dev\
  subversion

# Intall perf for kernel 3.13
# Also uses libunwind
RUN apt-get install -y flex bison libelf-dev libaudit-dev libdw-dev binutils-dev && \
  wget https://github.com/torvalds/linux/archive/v3.13.tar.gz && \
  tar -xvzf v3.13.tar.gz && rm v3.13.tar.gz && \
  cd linux-3.13/tools/perf && make && make install && \
  cp perf /usr/bin && \
  cp libperf.a /usr/lib && \
  cd / && rm -Rf /linux-3.13

ADD assets/bin/perfj /usr/bin/
ADD assets/lib/perfj-1.0.jar /usr/

# Flink
WORKDIR /software
RUN wget http://apache.cs.utah.edu/flink/flink-0.9.1/flink-0.9.1-bin-hadoop24.tgz && \
    tar -xvzf flink-0.9.1-bin-hadoop24.tgz
ADD jobmanager.sh /software/flink-0.9.1/bin/
ADD taskmanager.sh /software/flink-0.9.1/bin/

# Maven
WORKDIR /software
RUN wget http://mirrors.ibiblio.org/apache/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz && \
    tar -xzvf apache-maven-3.3.3-bin.tar.gz && \
    cp -r apache-maven-3.3.3/bin /usr/local/ && \
    cp -r apache-maven-3.3.3/boot /usr/local/ && \
    cp -r apache-maven-3.3.3/conf /usr/local/ && \
    cp -r apache-maven-3.3.3/lib /usr/local/ && \
    rm -rf apache-maven-3.3.3*

# Scala
WORKDIR /software
RUN curl -O http://www.scala-lang.org/files/archive/scala-2.10.4.tgz && \
    tar -xvzf scala-2.10.4.tgz
ENV SCALA_HOME /software/scala-2.10.4/bin/

# Startup Scripts
ADD master_startup.sh /sbin/
ADD slave_startup.sh /sbin/
ADD flink_perf_start.sh /sbin/
ADD flink_perf_stop.sh /sbin/
RUN chmod +x /sbin/master_startup.sh \
             /sbin/slave_startup.sh \
             /sbin/flink_perf_start.sh \
             /sbin/flink_perf_stop.sh \
             /usr/bin/perfj

# Replace nsswitch.conf: Prefer DNS over /etc/hosts
ADD nsswitch.conf /etc/

# Finish & Clean up
RUN apt-get remove -y gcc g++ make cmake git \
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

