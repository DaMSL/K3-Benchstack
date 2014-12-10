#Running TPCH queries in Storm
##Configure

Running storm requires a local build of the storm binaries. The included pom.xml is provides for use with Maven to compile & run locally as well as to package storm-tpch in a jar for deploying to cluster. TO use:

Install Storm (v 9.3.1), Maven, & upgrade to java 8 (or use the docker container)

Place storm-tpch in the STORM_HOME location (or update pom.xml) to locate rel-path of the Storm pom.xml

##Running

to run locally: mvn compile exec:java -Dstorm.topology=storm.starter.TPCHq01Topology

to package: mvn package

(work in progress)




