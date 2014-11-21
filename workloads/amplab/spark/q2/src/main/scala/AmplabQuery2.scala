import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

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

object AmplabQueryTwo {
  def main(args: Array[String]) {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("Amplab Query 2")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")
    val sc = new SparkContext(conf)
    val filePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/uservisits"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val uservisits = sc.textFile(filePath).map(_.split(",")).map(r => UserVisit(r(0),r(1),r(2),r(3).toDouble,r(4),r(5),r(6),r(7),r(8).toInt))

    uservisits.registerTempTable("uservisits")
    sqlContext.cacheTable("uservisits") 
    val r1 = sqlContext.sql("SELECT COUNT(*) FROM uservisits").collect()
    print(r1(0).toString)

    var start = System.currentTimeMillis
    val result =  sqlContext.sql("SELECT substr(sourceIP, 0, 8), SUM(adRevenue) FROM uservisits GROUP BY substr(sourceIP, 0, 8)")
    println("Num Results: " + result.count )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)

  }
}
