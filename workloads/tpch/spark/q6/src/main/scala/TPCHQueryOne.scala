import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

case class Lineitem(
  l_orderkey: Int,
  l_partkey:  Int,
  l_suppkey:  Int,
  l_linenumber: Int,
  l_quantity:  Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinsruct: String,
  l_shipmode: String,
  l_comments: String
)

object TPCHQueryOne {
  def main(args: Array[String]) {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("TPCH Query 6")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")

val sc = new SparkContext(conf)
    val filePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/lineitem"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val lineitems = sc.textFile(filePath).map(_.split("\\|")).map(r => Lineitem(r(0).toInt,r(1).toInt,r(2).toInt, r(3).toInt, r(4).toDouble, r(5).toDouble, r(6).toDouble, r(7).toDouble, r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15)))

    lineitems.registerTempTable("lineitem")
    sqlContext.cacheTable("lineitem") 
    val r1 = sqlContext.sql("SELECT COUNT(*) from lineitem").collect()
    print(r1(0).toString)

    var start = System.currentTimeMillis
    val query = """
     |select
     |   sum(l_extendedprice * l_discount) as revenue
     |from
     |   lineitem
     |where
     |   l_shipdate >= date '1994-01-01'
     |   and l_shipdate < date '1995-01-01'
     |   and l_discount between 0.06 - 0.01 and 0.06 + 0.01
     |   and l_quantity < 24 
     """.stripMargin
    val result =  sqlContext.sql(query)
    println("Num Results: " + result.count )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)

  }
}
