import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


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
    
    val createLineitem = """
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
    | LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/lineitem/'
    """.stripMargin
    
    val createCustomer = """
    | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
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
    | LOCATION 'hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch/10g/orders'
    """.stripMargin
    sqlContext.sql(createLineitem)
    sqlContext.sql(createCustomer)
    sqlContext.sql(createOrders) 


    val query = """
        | select  l.l_orderkey,
        |         sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
        |         o.o_orderdate,
        |         o.o_shippriority
        | from    customer as c
        | join    orders as o on c.c_custkey = o.o_custkey
        | join    lineitem as l on o.o_orderkey = l.l_orderkey
        | where
        |         c.c_mktsegment = "BUILDING"
        |         and o.o_orderdate < "1995-03-15"
        |         and l.l_shipdate >  "1995-03-15"
        | group by
        |         l.l_orderkey,
        |         o.o_orderdate,
        |         o.o_shippriority,
        |         o.o_orderdate
    """.stripMargin

    val start = System.currentTimeMillis
    val result =  sqlContext.sql(query).count()
    println("Num Results: " + result.toString )
    var end = System.currentTimeMillis

    println("Elapsed: " + (end - start).toString)
  }
}
