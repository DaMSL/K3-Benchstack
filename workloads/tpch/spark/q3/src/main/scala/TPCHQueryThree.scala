import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

val createLineitem = """
| CREATE EXTERNAL TABLE lineitem (
|   l_orderkey Int,
|   l_partkey Int,
|   l_suppkey Int,
|   l_lineitem Int,
|   l_quantity Double,
|   l_extendedprice Double,
|   l_discount Double,
|   l_tax Double,
|   l_returnflag String,
|   l_linestatus String,
|   l_shipdate String,
|   l_commitdate String,
|   l_receiptdate String,
|   l_shipinstruct String,
|   l_shipmode String,
|   l_comments String
| ) 
| ROW FORMAT DELIMITED
| FIELDS TERMINATED BY '|'
| LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/'
""".stripMargin

val createCustomer = """
| CREATE EXTERNAL TABLE customer (
|   c_custkey Int,
|   c_name String,
|   c_address String,
|   c_nationkey Int,
|   c_phone String,
|   c_acctbal Double,
|   c_mktsegment String,
|   c_comments String
| )
| ROW FORMAT DELIMITED
| FIELDS TERMINATED BY "|"
| LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/customer'
""".stripMargin

val createOrders = """
| CREATE EXTERNAL TABLE orders (
| o_orderkey Int,
| o_custkey Int,
| o_orderstatus String,
| o_totalprice Double,
| o_orderdate String,
| o_orderpriority String,
| o_clerk String,
| o_shippriority Int,
| o_comments String
| )
| ROW FORMAT DELIMITED
| FIELDS TERMINATED BY "|"
| LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/orders'
""".stripMargin

object TPCHQueryThree {
  def main(args: Array[String]) {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("TPCH Query 6")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")

    val sc = new SparkContext(conf)
    val filePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/lineitem"
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    sqlContext.sql(createLineitem)
    sqlContext.sql(createCustomer)
    sqlContext.sql(createOrders) 

    val result =  sqlContext.sql("SELECT COUNT(*) FROM rankings").collect()
    println("Num Results: " + result.toString )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)
  }
}
