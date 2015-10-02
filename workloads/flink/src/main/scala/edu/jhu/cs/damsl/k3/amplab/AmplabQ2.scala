package edu.jhu.cs.damsl.k3.amplab

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object AmplabQ2 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val results = getUserVisitsDataSet(env)
                    .groupBy(u => u.sourceIP.substring(0,substrlen))
                    .sum(1)
                    .map(u => (u.sourceIP.substring(0,substrlen), u.adRevenue))

    results.writeAsText(outputPath, WriteMode.OVERWRITE)
    env.execute("Scala Amplab Q2")
  }
  
  case class UserVisits(sourceIP  : String,
                        adRevenue : Double)  
  
  private var substrlen: Int = 8
  private var userVisitsPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 2 && args.length < 4) {
      userVisitsPath = args(0)
      outputPath = args(1)
      if ( args.length == 3 ) { substrlen = args(2).toInt }
      true
    } else {
      System.err.println("Usage: AmplabQ2 <uservisits-csv path> <result path> [length]")
      false
    }
  }

  private def getUserVisitsDataSet(env: ExecutionEnvironment): DataSet[UserVisits] = {
    env.readCsvFile[UserVisits](
        userVisitsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 3) )
  }
}