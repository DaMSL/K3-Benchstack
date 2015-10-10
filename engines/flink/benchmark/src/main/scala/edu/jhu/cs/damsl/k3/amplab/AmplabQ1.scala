package edu.jhu.cs.damsl.k3.amplab

import edu.jhu.cs.damsl.k3.common.AmplabDeployment

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object AmplabQ1 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val results = getQ1RankingsDataSet(env).filter(r => r.pageRank > threshold)
    results.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala AmplabQ1"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }
  
  case class Rankings(pageURL  : String,
                      pageRank : Int)  
  
  private var threshold: Double = 1000.0
  private var deployment : AmplabDeployment = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 1 && args.length < 3) {
      deployment = new AmplabDeployment(args(0))
      if ( args.length == 2 ) { threshold = args(1).toDouble }
      true
    } else {
      System.err.println("Usage: AmplabQ1 <result path> [threshold]")
      false
    }
  }

  def getQ1RankingsDataSet(env: ExecutionEnvironment) : DataSet[Rankings] = {
    env.readCsvFile[Rankings](
        deployment.rankingsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1) )
  }
}