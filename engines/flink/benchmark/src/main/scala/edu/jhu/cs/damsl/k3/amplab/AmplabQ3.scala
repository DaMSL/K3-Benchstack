package edu.jhu.cs.damsl.k3.amplab

import edu.jhu.cs.damsl.k3.common.AmplabDeployment

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
    
    val results = getQ3UserVisitsDataSet(env).filter(v => {
        val d = dateFormat.parse(v.visitDate)
        d.equals(dateLB) || d.equals(dateUB) || d.after(dateLB) && d.before(dateUB)
      })
      .join(getQ3RankingsDataSet(env)).where(1).equalTo(0)
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
     
    results.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

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
 
  private var deployment : AmplabDeployment = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 1 && args.length < 3) {
      deployment = new AmplabDeployment(args(0))
      if ( args.length == 2 ) { dateUpper = args(1) }
      true
    } else {
      System.err.println("Usage: AmplabQ3 <result path> [date-upper]")
      false
    }
  }

  def getQ3RankingsDataSet(env: ExecutionEnvironment) : DataSet[Rankings] = {
    env.readCsvFile[Rankings](
        deployment.rankingsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1) )
  }

  def getQ3UserVisitsDataSet(env: ExecutionEnvironment) : DataSet[UserVisits] = {
    env.readCsvFile[UserVisits](
        deployment.userVisitsPath,
        fieldDelimiter = ",",
        includedFields = Array(0, 1, 2, 3) )
  }

}