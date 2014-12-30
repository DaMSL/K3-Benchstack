FROM ubuntu:latest

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apt-get update && apt-get install -y git vim wget python postgresql-client-9.3 python-psycopg2 default-jdk python-matplotlib xvfb
RUN wget http://archive.cloudera.com/cdh5/one-click-install/trusty/amd64/cdh5-repository_1.0_all.deb
RUN dpkg -i cdh5-repository_1.0_all.deb
RUN apt-get update && apt-get install -y impala-shell

RUN git clone https://github.com/damsl/k3-benchstack

RUN mkdir /software
ADD vertica-client-7.1.1-0.linux.x86_64.tar.gz /software/

RUN ln -s /software/opt/vertica/bin/vsql /usr/bin/vsql
ADD entrypoint.sh /
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
