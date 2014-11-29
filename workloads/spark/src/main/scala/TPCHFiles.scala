import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

package tpch.files {

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

object TPCHFiles {
  val lineitemPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/lineitem"
  
  val ordersHivePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/orders/"
  val lineitemHivePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/"
  val customerHivePath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/customer/"
  
  def getLineitem(sc: SparkContext) = {
    val csv = sc.textFile(lineitemPath).map(_.split("\\|"))
    csv.map(r => Lineitem(r(0).toInt,r(1).toInt,r(2).toInt, r(3).toInt, r(4).toDouble, r(5).toDouble, r(6).toDouble, r(7).toDouble, r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15)))

  }

  def cacheLineitem(sc: SparkContext, sqlContext: SQLContext) = {
      import sqlContext._
      val lineitem = getLineitem(sc)
      lineitem.registerTempTable("lineitem")
      sqlContext.cacheTable("lineitem") 
      val r1 = sqlContext.sql("SELECT COUNT(*) from lineitem").collect()
      print("Cached Lineitem. #Rows: " + r1(0).toString)
  }

  // Hive on Spark

  def cacheLineitemHive(sqlContext: HiveContext) = {
    val create_query = s"""
    | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
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
    | LOCATION '$lineitemHivePath'
      """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("lineitem")
    val r1 = sqlContext.sql("SELECT COUNT(*) from lineitem").collect()
    print("Cached Lineitem. #Rows: " + r1(0).toString)
  }

  def cacheOrdersHive(sqlContext: HiveContext) = {
    val create_query = s"""
    | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
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
    | LOCATION '$ordersHivePath'
      """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("orders")
    val r1 = sqlContext.sql("SELECT COUNT(*) from orders").collect()
    print("Cached Orders. #Rows: " + r1(0).toString)
  }
  
  def cacheCustomerHive(sqlContext: HiveContext) = {
    val create_query = s"""
    | create external table if not exists customer (
    |   c_custkey int,
    |   c_name string,
    |   c_address string,
    |   c_nationkey int,
    |   c_phone string,
    |   c_acctbal double,
    |   c_mktsegment string,
    |   c_comments string
    | )
    | row format delimited
    | fields terminated by "|"
    | location '$customerHivePath'
    """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("customer")
    val r1 = sqlContext.sql("SELECT COUNT(*) from customer").collect()
    print("Cached Customer. #Rows: " + r1(0).toString)
  }

}

}
