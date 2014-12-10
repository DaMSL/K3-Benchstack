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

    // TODO force cache

    // JOIN nation, partsupp, supplier and filter nation = GERMANY 
    val ps_s = ps.join(s).where('ps_suppkey === 's_suppkey)
    val ps_s_n = ps_s.join(n.where('n_name === "GERMANY")).where('s_nationkey === 'n_nationkey).cache()

    // Calculate total sum, forcing the 3 way join into cache
    val total_sum = ps_s_n.aggregate(Sum('ps_supplycost * 'ps_availqty) as 'value)
    val c = total_sum.collect()
    val threshold = c(0)(0).toString.toDouble * .0001
    println("THRESHOLD:")
    println(c(0).toString)
    println(threshold.toString)

    // Use cached join result, perform groupby and filter
    val result = ps_s_n.groupBy('ps_partkey)( Sum('ps_supplycost * 'ps_availqty) as 'value)
   
    result.collect().foreach(println)
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
    // TODO force cache

    val l_agg : SchemaRDD = lineitem.groupBy('l_orderkey)('l_orderkey as 'la_orderkey, Sum('l_quantity) as 'sum).where('sum > 300)

    val orders2 = orders.join(l_agg).where('la_orderkey === 'o_orderkey)
    val join2 = orders2.join(customer).where('o_custkey === 'c_custkey)
    val join3 = join2.join(lineitem).where('o_orderkey === 'l_orderkey)
    
    val result = join3.groupBy('c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice)('c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice, Sum('l_quantity))

    result.collect().foreach(println)

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
    val r = customer.where('c_phone)((p : String) => codes.contains(p.substring(0,2))).where('c_acctbal > 0.0)
    val r2 = r.aggregate(Average('c_acctbal)) 
    val avg_bal : Double  = r2.collect()(0)(0).asInstanceOf[Double]
    println("====================")
    println(avg_bal)

    val res = customer.where('c_acctbal > avg_bal).join(orders, org.apache.spark.sql.catalyst.plans.LeftOuter, Some('o_custkey === 'c_custkey))
    //where('o_custkey === null).where('c_acctbal > avg_bal)
  
    res.collect().foreach(println)

  }
}

