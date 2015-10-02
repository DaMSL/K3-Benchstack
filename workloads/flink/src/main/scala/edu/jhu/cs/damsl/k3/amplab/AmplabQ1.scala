package edu.jhu.cs.damsl.k3.amplab

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object AmplabQ1 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val results = getRankingsDataSet(env).filter(r => r.pageRank > threshold)
    results.writeAsText(outputPath, WriteMode.OVERWRITE)
    env.execute("Scala Amplab Q1")
  }
  
  case class Rankings(pageURL  : String,
                      pageRank : Int)  
  
  private var threshold: Double = 1000.0
  private var rankingsPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 2 && args.length < 4) {
      rankingsPath = args(0)
      outputPath = args(1)
      if ( args.length == 3 ) { threshold = args(2).toDouble }
      true
    } else {
      System.err.println("Usage: AmplabQ1 <rankings-csv path> <result path> [threshold]")
      false
    }
  }

  private def getRankingsDataSet(env: ExecutionEnvironment): DataSet[Rankings] = {
    env.readCsvFile[Rankings](
        rankingsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1) )
  }
}