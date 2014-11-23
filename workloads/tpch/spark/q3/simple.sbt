name := "TPCH Q3"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
