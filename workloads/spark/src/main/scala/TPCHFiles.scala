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

case class Partsupp (
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comments: String
) 

case class Supplier (
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comments: String
)

case class Nation (
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comments: String
)

case class Customer (
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comments: String
)

case class Order (
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comments: String
)

object TPCHFiles {
  def getLineitem(sc: SparkContext, sf: String) = {
    val lineitemPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/lineitem/lineitem"
    val csv = sc.textFile(lineitemPath).map(_.split("\\|"))
    csv.map(r => Lineitem(r(0).toInt,r(1).toInt,r(2).toInt, r(3).toInt, r(4).toDouble, r(5).toDouble, r(6).toDouble, r(7).toDouble, r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15)))

  }
  
  def getPartsupp(sc: SparkContext, sf: String) = {
    val partsuppPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/partsupp/partsupp"
    val csv = sc.textFile(partsuppPath).map(_.split("\\|"))
    csv.map(r => Partsupp(r(0).toInt,r(1).toInt,r(2).toInt, r(3).toDouble, r(4)))
  }
  
  def getNation(sc: SparkContext, sf: String) = {
    val nationPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/nation/nation"
    val csv = sc.textFile(nationPath).map(_.split("\\|"))
    csv.map(r => Nation(r(0).toInt,r(1),r(2).toInt, r(3)))
  }
  
  def getSupplier(sc: SparkContext, sf: String) = {
    val supplierPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/supplier/supplier"
    val csv = sc.textFile(supplierPath).map(_.split("\\|"))
    csv.map(r => Supplier(r(0).toInt,r(1),r(2), r(3).toInt,r(4), r(5).toDouble, r(6) ))
  }
  
  def getCustomer(sc: SparkContext, sf: String) = {
    val customerPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/customer/customer"
    val csv = sc.textFile(customerPath).map(_.split("\\|"))
    csv.map(r => Customer(r(0).toInt,r(1),r(2), r(3).toInt,r(4), r(5).toDouble, r(6), r(7) ))
  }
  
  def getOrders(sc: SparkContext, sf: String) = {
    val ordersPath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/orders/orders"
    val csv = sc.textFile(ordersPath).map(_.split("\\|"))
    csv.map(r => Order(r(0).toInt,r(1).toInt ,r(2), r(3).toDouble,r(4), r(5), r(6), r(7).toInt, r(8) ))
  }

  def cacheLineitem(sc: SparkContext, sqlContext: SQLContext, sf: String) = {
      import sqlContext._
      val lineitem = getLineitem(sc, sf)
      lineitem.registerTempTable("lineitem")
      sqlContext.cacheTable("lineitem") 
      val r1 = sqlContext.sql("SELECT COUNT(*) from lineitem").collect()
      print("Cached Lineitem. #Rows: " + r1(0).toString)
  }

  // Hive on Spark

  def cacheLineitemHive(sqlContext: HiveContext, sf: String, sf: String) = {
    val lineitemHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/lineitem/"
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

  def cacheOrdersHive(sqlContext: HiveContext, sf: String) = {
    val ordersHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/orders/"
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
  
  def cacheCustomerHive(sqlContext: HiveContext, sf: String) = {
    val customerHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/customer/"
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

  def cacheSupplierHive(sqlContext: HiveContext, sf: String) = {
    val supplierHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/supplier/"
    val create_query = s"""
    | create external table if not exists supplier (
    |   s_suppkey int,
    |   s_name string,
    |   s_address string,
    |   s_nationkey int,
    |   s_phone string,
    |   s_acctbal double,
    |   s_comments string
    | )
    | row format delimited
    | fields terminated by "|"
    | location '$supplierHivePath'
    """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("supplier")
    val r1 = sqlContext.sql("SELECT COUNT(*) from supplier").collect()
    print("Cached Supplier. #Rows: " + r1(0).toString)
  }

  def cacheRegionHive(sqlContext: HiveContext, sf: String) = {
    val regionHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/region/"
    val create_query = s"""
    | create external table if not exists region (
    |   r_regionkey int,
    |   r_name string,
    |   r_comments string
    | )
    | row format delimited
    | fields terminated by "|"
    | location '$regionHivePath'
    """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("region")
    val r1 = sqlContext.sql("SELECT COUNT(*) from region").collect()
    print("Cached Region. #Rows: " + r1(0).toString)
  }
  
  def cacheNationHive(sqlContext: HiveContext, sf: String) = {
    val nationHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/nation/"
    val create_query = s"""
    | create external table if not exists nation (
    |   n_nationkey int,
    |   n_name string,
    |   n_regionkey int,
    |   n_comments string 
    | )
    | row format delimited
    | fields terminated by "|"
    | location '$nationHivePath'
    """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("nation")
    val r1 = sqlContext.sql("SELECT COUNT(*) from nation").collect()
    print("Cached Nation. #Rows: " + r1(0).toString)
  }
  
  def cachePartsuppHive(sqlContext: HiveContext, sf: String) = {
    val partsuppHivePath = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/$sf/partsupp/"
    val create_query = s"""
    | create external table if not exists partsupp (
    |   n_partsuppkey int,
    |   n_name string,
    |   n_regionkey int,
    |   n_comments string 
    | )
    | row format delimited
    | fields terminated by "|"
    | location '$partsuppHivePath'
    """.stripMargin
  
    sqlContext.sql(create_query)
    sqlContext.cacheTable("partsupp")
    val r1 = sqlContext.sql("SELECT COUNT(*) from partsupp").collect()
    print("Cached Partsupp. #Rows: " + r1(0).toString)
  }
}

}
