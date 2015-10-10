#!/bin/bash

# Run this script inside a docker container, with /src and /flink mounted.

cd /src && mvn clean package -Pbuild-jar
cp target/flink-tpch-1.0-SNAPSHOT.jar /flink/
