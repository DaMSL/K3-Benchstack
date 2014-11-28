import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import amplab.schema._
import amplab.files._

object Config {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("Queries")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    def timeSqlQuery(query: String) = {
      // Start timer
      var start = System.currentTimeMillis

      // Run Query
      val result =  sqlContext.sql(query)

      // Force evaluation with .count
      println("Num Results: " + result.count )

      // Stop timer
      var end = System.currentTimeMillis
      println("Elapsed: " + (end - start).toString)
    }

}

object AmplabQuery1 {
  def main(args: Array[String]) {
    // Config
    val sc = Config.sc
    val sqlContext = Config.sqlContext   
    import sqlContext.createSchemaRDD
    val rankings = AmplabFiles.getRankings(sc)

    // Create table and cache
    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings") 
    val r1 = sqlContext.sql("SELECT COUNT(*) from rankings").collect()
    print(r1(0).toString)

    // Query with timing
    val query = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000"
    Config.timeSqlQuery(query)
  }
}

object AmplabQuery2 {
  def main(args: Array[String]) {
    // Config
    val sc = Config.sc
    val sqlContext = Config.sqlContext
    import sqlContext.createSchemaRDD
    val uservisits = AmplabFiles.getUservisits(sc) 

    // Create table and cache
    uservisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits") 
    val r1 = sqlContext.sql("SELECT COUNT(*) FROM uservisits").collect()
    print(r1(0).toString)

    // Query with timing
    val query = "SELECT substr(sourceIP, 0, 8), SUM(adRevenue) FROM uservisits GROUP BY substr(sourceIP, 0, 8)" 
   Config.timeSqlQuery(query)

  }
}

object AmplabQuery3 {
  def main(args: Array[String]) {
    // Config
    val sc = Config.sc
    val sqlContext = Config.sqlContext
    import sqlContext.createSchemaRDD
    val rankings = AmplabFiles.getRankings(sc)
    val uservisits = AmplabFiles.getUservisits(sc)

    // Create and cache 'rankings'
    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings") 
    val r1 = sqlContext.sql("SELECT COUNT(*) from rankings").collect()
    print(r1(0).toString)

    // Create and cache uservisits 
    uservisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits") 
    val r2 = sqlContext.sql("SELECT COUNT(*) FROM uservisits").collect()
    print(r2(0).toString)

    // Query with timing
    val query = """
    |SELECT t.sourceIP, t.totalRevenue, t.avgPageRank
    |FROM
    | (SELECT sourceIP, AVG(pageRank) as avgPageRank, SUM(adRevenue) as totalRevenue
    |   FROM rankings AS R, uservisits AS UV
    |   WHERE R.pageURL = UV.destURL
    |   AND UV.visitDate >= "1980-01-01" AND UV.visitDate <= "1980-04-01"
    |   GROUP BY UV.sourceIP) t
    |ORDER BY totalRevenue DESC LIMIT 1""".stripMargin

    Config.timeSqlQuery(query)
  }

}
