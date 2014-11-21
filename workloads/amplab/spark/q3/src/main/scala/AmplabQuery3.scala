import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


case class Ranking(pageURL: String, pageRank: Int, avgDuration: Int) 

case class UserVisit(
  sourceIP: String,
  destURL: String,
  visitDate: String,
  adRevenue: Double,
  userAgent: String,
  countryCode: String,
  languageCode: String,
  searchWord: String,
  duration: Int
)

object AmplabQueryThree {
  def main(args: Array[String]) {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("Amplab Query 2")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")
    val sc = new SparkContext(conf)
    val uservisitsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/uservisits"
    val rankingsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/rankings"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val rankings = sc.textFile(rankingsPath).map(_.split(",")).map(r => Ranking(r(0),r(1).toInt,r(2).toInt))

    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings") 
    val r1 = sqlContext.sql("SELECT COUNT(*) from rankings").collect()
    print(r1(0).toString)
    val uservisits = sc.textFile(uservisitsPath).map(_.split(",")).map(r => UserVisit(r(0),r(1),r(2),r(3).toDouble,r(4),r(5),r(6),r(7),r(8).toInt))

    uservisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits") 
    val r2 = sqlContext.sql("SELECT COUNT(*) FROM uservisits").collect()
    print(r2(0).toString)

    var start = System.currentTimeMillis
    val query = """
    |SELECT t.sourceIP, t.totalRevenue, t.avgPageRank
    |FROM
    | (SELECT sourceIP, AVG(pageRank) as avgPageRank, SUM(adRevenue) as totalRevenue
    |   FROM rankings AS R, uservisits AS UV
    |   WHERE R.pageURL = UV.destURL
    |   AND UV.visitDate >= "1980-01-01" AND UV.visitDate <= "1980-04-01"
    |   GROUP BY UV.sourceIP) t
    |ORDER BY totalRevenue DESC LIMIT 1""".stripMargin

    val result =  sqlContext.sql(query)
    println("Num Results: " + result.count )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)

  }
}
