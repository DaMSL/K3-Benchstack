import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import amplab.files._
import common._

object AmplabQuery1 {
  def main(args: Array[String]) {
    val sc = Common.sc
    val sqlContext = Common.sqlContext   

    AmplabFiles.cacheRankings(sc,sqlContext)

    val query = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000"
    Common.timeSqlQuery(query, "amplab/q1")
  }
}

object AmplabQuery2 {
  def main(args: Array[String]) {
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    import sqlContext.createSchemaRDD

    AmplabFiles.cacheUservisits(sc,sqlContext)

    val query = "SELECT substr(sourceIP, 0, 8), SUM(adRevenue) FROM uservisits GROUP BY substr(sourceIP, 0, 8)" 
    Common.timeSqlQuery(query, "amplab/q2")
  }
}

object AmplabQuery3 {
  def main(args: Array[String]) {
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    import sqlContext.createSchemaRDD

    AmplabFiles.cacheRankings(sc,sqlContext)
    AmplabFiles.cacheUservisits(sc,sqlContext)

    val query = """
    |SELECT t.sourceIP, t.totalRevenue, t.avgPageRank
    |FROM
    | (SELECT sourceIP, AVG(pageRank) as avgPageRank, SUM(adRevenue) as totalRevenue
    |   FROM rankings AS R, uservisits AS UV
    |   WHERE R.pageURL = UV.destURL
    |   AND UV.visitDate >= "1980-01-01" AND UV.visitDate <= "1980-04-01"
    |   GROUP BY UV.sourceIP) t
    |ORDER BY totalRevenue DESC LIMIT 1""".stripMargin
    Common.timeSqlQuery(query, "amplab/q3")
  }
}
