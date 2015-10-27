package edu.jhu.cs.damsl.k3.common

import org.apache.flink.api.scala._

class AmplabDeployment(var outputPath: String)
{
  val rankingsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/"
  val userVisitsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/"
}
