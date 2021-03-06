/* Schemas and File Paths for Amplab Queries 1-3 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package amplab.files {

  /* Schema */
  case class Ranking (
    pageURL:     String,
    pageRank:    Int, 
    avgDuration: Int 
  )

  case class UserVisit (
    sourceIP:     String,
    destURL:      String,
    visitDate:    String,
    adRevenue:    Double,
    userAgent:    String,
    countryCode:  String,
    languageCode: String,
    searchWord:   String,
    duration:     Int
  )

  /* File Paths & Load/Parse/Cache Utils */
  object AmplabFiles {
    val rankingsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/"
    val uservisitsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/"

    /* RDD Creation */
    def getRankings(sc: SparkContext) = {
      val csv  = sc.textFile(rankingsPath).map(_.split(","))
      csv.map(r => Ranking(
                     r(0), 
                     r(1).toInt, 
                     r(2).toInt
                   )
             )
    }

    def getUservisits(sc: SparkContext) = {
      val csv = sc.textFile(uservisitsPath).map(_.split(","))
      csv.map(r => UserVisit(
                      r(0),
                      r(1),
                      r(2),
                      r(3).toDouble,
                      r(4),
                      r(5),
                      r(6),
                      r(7),
                      r(8).toInt
                    )
             )
    }

    /* SparkSQL RDD Caching, using a simple count(*) to force evaluation */
    def cacheRankings(sc: SparkContext, sqlContext: SQLContext) = {
      import sqlContext._
      val rankings = getRankings(sc)
      rankings.registerTempTable("rankings")
      sqlContext.cacheTable("rankings") 
      val r1 = sqlContext.sql("SELECT COUNT(*) from rankings").collect()
      print("Cached Rankings. #Rows: " + r1(0).toString)
    }
    
    def cacheUservisits(sc: SparkContext, sqlContext: SQLContext) = {
      import sqlContext._
      val uservisits = getUservisits(sc)
      uservisits.registerTempTable("uservisits")
      sqlContext.cacheTable("uservisits") 
      val r1 = sqlContext.sql("SELECT COUNT(*) from uservisits").collect()
    }
  }
}
