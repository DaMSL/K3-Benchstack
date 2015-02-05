import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import tpch.files._
import common._

object TPCHQuery1 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext   
    val sf = args(0)
    TPCHFiles.cacheLineitem(sc,sqlContext, sf)

    println("RUNNING QUERY 1")

    // Query with timing
    val query = """
      |select
      |        l_returnflag,
      |        l_linestatus,
      |        sum(l_quantity) as sum_qty,
      |        sum(l_extendedprice) as sum_base_price,
      |        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |        avg(l_quantity) as avg_qty,
      |        avg(l_extendedprice) as avg_price,
      |        avg(l_discount) as avg_disc,
      |        count(*) as count_order
      |from
      |        lineitem
      |where
      |        l_shipdate <= "1998-09-02"
      |group by
      |        l_returnflag,
      |        l_linestatus
      """.stripMargin
   Common.timeSqlQuery(query, s"tpch/q1$sf")
  }
}

object TPCHQuery5 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext   
    val sf = args(0)

    TPCHFiles.cacheCustomerHive(sqlContext, sf)
    TPCHFiles.cacheOrdersHive(sqlContext, sf)
    TPCHFiles.cacheLineitemHive(sqlContext, sf)
    TPCHFiles.cacheSupplierHive(sqlContext, sf)
    TPCHFiles.cacheNationHive(sqlContext, sf)
    TPCHFiles.cacheRegionHive(sqlContext, sf)

    // Query with timing 
    val query = """
      | select
      |   n.n_name,
      |   sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
      | from
      |   customer as c
      |   join orders as o on c.c_custkey = o.o_custkey
      |   join lineitem as l on l.l_orderkey = o.o_orderkey
      |   join supplier as s on l.l_suppkey = s.s_suppkey and c.c_nationkey = s.s_nationkey
      |   join nation as n on s.s_nationkey = n.n_nationkey
      |   join region as r on n_regionkey = r_regionkey
      | where
      |   r.r_name = 'ASIA'
      |   and o.o_orderdate >= '1994-01-01'
      |   and o.o_orderdate <  '1995-01-01'
      | group by
      |   n.n_name  
     """.stripMargin
    Common.timeHiveQuery(query, "tpch/q5")
  }
}

object TPCHQuery6 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    val sf = args(0)

    TPCHFiles.cacheLineitem(sc,sqlContext, sf)

    // Query with timing
    val query = """
     |select
     |   sum(l_extendedprice * l_discount) as revenue
     |from
     |   lineitem
     |where
     |   l_shipdate >= "1994-01-01"
     |   and l_shipdate < "1995-01-01"
     |   and l_discount >= 0.05 
     |   and l_discount <= 0.07
     |   and l_quantity < 24 
     """.stripMargin
    Common.timeSqlQuery(query, s"tpch/q6$sf")
  }
}

object TPCHQuery3 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext
    val sf = args(0) 
    
    TPCHFiles.cacheLineitemHive(sqlContext, sf)
    TPCHFiles.cacheCustomerHive(sqlContext, sf)
    TPCHFiles.cacheOrdersHive(sqlContext, sf)

    // Query with timing
    val query = """
      | select  l.l_orderkey,
      |         sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
      |         o.o_orderdate,
      |         o.o_shippriority
      | from    customer as c
      | join    orders   as o on c.c_custkey = o.o_custkey
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
    Common.timeHiveQuery(query, s"tpch/q3$sf")
  }
}

object TPCHQuery11 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    val sf = args(0)
    import sqlContext._ 

    // Load base tables
    val ps = TPCHFiles.getPartsupp(sc, sf).cache()
    val s  = TPCHFiles.getSupplier(sc, sf).cache()
    val n  = TPCHFiles.getNation(sc, sf).cache()

    // Force cache
    println(ps.count())
    println(s.count())
    println(n.count()) 

    val start = System.currentTimeMillis

    // Compute the constant result of the subquery for use in the main query:
    //   JOIN   nation, partsupp, supplier
    //   FILTER nation = GERMANY 
    //   Cache the result for re-use in the main query. 
    val ps_s = ps.join(s).where('ps_suppkey === 's_suppkey)
    val ps_s_n = ps_s.join(n.where('n_name === "GERMANY")).where('s_nationkey === 'n_nationkey).cache()

    val total_sum = ps_s_n.aggregate(Sum('ps_supplycost * 'ps_availqty) as 'value)
    val c = total_sum.collect()
    val threshold = c(0)(0).toString.toDouble * .0001

    // Use cached join result to perform groupby and filter
    val result = ps_s_n.groupBy('ps_partkey)( 'ps_partkey, Sum('ps_supplycost * 'ps_availqty) as 'value).where('value > threshold) 
  
    // Force evaluation 
    val num_results = result.count()
    val end = System.currentTimeMillis

    println("====== START PLAN ---->>")
    //println(total_sum.queryExecution.executedPlan.toString())
    println(result.queryExecution.executedPlan.toString())
    println("<<---- END PLAN   ======")
    println("Num Results: " + num_results )
    println("Elapsed: " + (end - start).toString())


    // TODO save result to HDFS 
  }
}
object TPCHQuery18 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    val sf = args(0)
    import sqlContext._ 

    // Load base tables
    val lineitem : SchemaRDD = TPCHFiles.getLineitem(sc, sf).cache()
    val orders : SchemaRDD = TPCHFiles.getOrders(sc, sf).cache()
    val customer : SchemaRDD = TPCHFiles.getCustomer(sc, sf).cache()
    // Force cache
    println(lineitem.count())
    println(orders.count())
    println(customer.count())

    val start = System.currentTimeMillis

    val l_agg : SchemaRDD = lineitem.groupBy('l_orderkey)('l_orderkey as 'la_orderkey, Sum('l_quantity) as 'sum).where('sum > 300)
    val join1 = orders.join(customer).where('o_custkey === 'c_custkey)
    val join2 = lineitem.join(join1).where('l_orderkey === 'o_orderkey)
    val join3 = l_agg.join(join2).where('la_orderkey === 'o_orderkey)

    val result = join3.groupBy('c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice)('c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice, Sum('l_quantity))

    // Force evaluation
    val num_results = result.count()
    val end = System.currentTimeMillis

    println("====== START PLAN ---->>")
    println(result.queryExecution.executedPlan.toString())
    println("<<---- END PLAN   ======")
    println("Num Results: " + num_results )
    println("Elapsed: " + (end - start).toString())

    // TODO save result to HDFS
  }
}

object TPCHQuery22 {
  def main(args: Array[String]) {
    val codes = List("13", "31", "23", "29", "30", "18", "17")
    
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext
    val sf = args(0)
    import sqlContext._ 

    // Load base tables
    val lineitem : SchemaRDD = TPCHFiles.getLineitem(sc, sf).cache()
    val orders : SchemaRDD = TPCHFiles.getOrders(sc, sf).cache()
    val customer : SchemaRDD = TPCHFiles.getCustomer(sc, sf).cache()

    // force cache
    println(lineitem.count())
    println(orders.count())
    println(customer.count())

    val start = System.currentTimeMillis
    // Subquery: find average balance
    val r = customer.where('c_phone)((p : String) => codes.contains(p.substring(0,2))).where('c_acctbal > 0.0)
    val r2 = r.aggregate(Average('c_acctbal)) 
    val avg_bal : Double  = r2.collect()(0)(0).asInstanceOf[Double]

    // WHERE NOT EXISTS: Left-outer join where right == NULL
    val res = customer.where('c_phone)((p : String) => codes.contains(p.substring(0,2))).where('c_acctbal > avg_bal).join(orders, org.apache.spark.sql.catalyst.plans.LeftOuter, Some('o_custkey === 'c_custkey)).filter(r => r.isNullAt(r.length - 1)).select('c_phone, 'c_acctbal)

    // Final groupBy using regular RDD transformations
    val res2 = res.map(r => ( r.getString(0).substring(0,2), (1, r.getDouble(1)) )).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)) 

    val num_results = res2.count()
    val end = System.currentTimeMillis
    println("====== START PLAN ---->>")
    println("<<---- END PLAN   ======")
    println("Num Results: " + num_results.toString)
    println("Elapsed: " + (end - start).toString) 
    

    // TODO save result to HDFS

  }
}

