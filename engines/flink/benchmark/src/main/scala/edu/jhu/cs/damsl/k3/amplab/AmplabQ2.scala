package edu.jhu.cs.damsl.k3.amplab

import org.apache.flink.api.scala._
import org.apache.flink.util._
import org.apache.flink.api.common.functions._
import scala.collection.JavaConverters._
import org.apache.flink.core.fs.FileSystem.WriteMode

object AmplabQ2 {
  def safesub(s : String) : String = {
    if ( s.length() < substrlen ) { s }
    else { s.substring(0,substrlen) }
  }
  
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val results = getUserVisitsDataSet(env)
                    .groupBy(u => safesub(u.sourceIP))
                    .reduceGroup(new GroupReduceFunction[UserVisits, (String, Double)]() {
                      override def reduce(in: java.lang.Iterable[UserVisits], out: Collector[(String,Double)]) = {
                        var sub : Option[String] = None
                        var sum : Double = 0.0
                        for (r <- in.asScala) {
                          if ( sub.isEmpty ) { sub = Some(safesub(r.sourceIP)) }
                          sum += r.adRevenue
                        }
                        if ( sub.isDefined ) { out.collect((sub.get,sum)) }
                      }
                    })

    results.writeAsText(outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala AmplabQ2"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
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