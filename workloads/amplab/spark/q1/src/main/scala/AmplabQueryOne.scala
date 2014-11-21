import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

case class Ranking(pageURL: String, pageRank: Int, avgDuration: Int) 

object AmplabQueryOne {
  def main(args: Array[String]) {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("Test Program")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")
    val sc = new SparkContext(conf)
    val filePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/rankings"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val rankings = sc.textFile(filePath).map(_.split(",")).map(r => Ranking(r(0),r(1).toInt,r(2).toInt))

    rankings.registerTempTable("rankings")
    sqlContext.cacheTable("rankings") 

    var start = System.currentTimeMillis
    val result =  sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000")
    println("Num Results: " + result.count )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)

  }
}
