package edu.jhu.cs.damsl.k3.amplab

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.core.fs.FileSystem.WriteMode

object AmplabQ3 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val dateLB = dateFormat.parse("1980-01-01")
    val dateUB = dateFormat.parse(dateUpper)

    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val results = getUserVisitsDataSet(env).filter(v => {
        val d = dateFormat.parse(v.visitDate)
        d.equals(dateLB) || d.equals(dateUB) || d.after(dateLB) && d.before(dateUB)
      })
      .join(getRankingsDataSet(env)).where(1).equalTo(0)
      .apply((v:UserVisits,r:Rankings,out:Collector[(String,Int,Double)]) => out.collect((v.sourceIP, r.pageRank, v.adRevenue)))
      .groupBy(0)
      .reduceGroup(new GroupReduceFunction[(String, Int, Double), (String, Long, Double, Long)]() {
        override def reduce(in: java.lang.Iterable[(String, Int, Double)], out: Collector[(String, Long, Double, Long)]) = {
          var group : Option[String] = None
          var sum_pr : Long = 0
          var sum_ar : Double = 0
          var cnt : Long = 0
          for (r <- in.asScala) {
            if (group.isEmpty) { group = Some(r._1) } 
            sum_pr += r._2
            sum_ar += r._3
            cnt += 1           
          }
          if ( group.isDefined ) { out.collect((group.get, sum_pr, sum_ar, cnt)) }
        }
      })
      .map(x => (x._1, x._3, x._2/x._4))
     
    results.writeAsText(outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala AmplabQ3"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }

  case class Rankings(pageURL  : String,
                      pageRank : Int)  

  case class UserVisits(sourceIP  : String,
                        destURL   : String,
                        visitDate : String,
                        adRevenue : Double)
  
  private var dateUpper: String = "1980-04-01"
  private var rankingsPath: String = null
  private var userVisitsPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 3 && args.length < 5) {
      rankingsPath   = args(0)
      userVisitsPath = args(1)
      outputPath     = args(2)
      if ( args.length == 4 ) { dateUpper = args(3) }
      true
    } else {
      System.err.println("Usage: AmplabQ3 <rankings-csv path> <uservisits-csv path> <result path> [date-upper]")
      false
    }
  }
  
  private def getRankingsDataSet(env: ExecutionEnvironment): DataSet[Rankings] = {
    env.readCsvFile[Rankings](
        rankingsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1) )
  }

  private def getUserVisitsDataSet(env: ExecutionEnvironment): DataSet[UserVisits] = {
    env.readCsvFile[UserVisits](
        userVisitsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1, 2, 3) )
  }

}