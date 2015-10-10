package edu.jhu.cs.damsl.k3.common

import org.apache.flink.api.scala._

class MLDeployment(var scaleFactor: String, var outputPath: String)
{
  def pointsPath(sf: String) = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/sgd_single_file/$sf/sgd$sf" }
  def this() { this("10g", "/query") }
}