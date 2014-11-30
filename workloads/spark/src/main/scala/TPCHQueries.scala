import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import tpch.files._
import common._

object TPCHQuery1 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.sqlContext   
    TPCHFiles.cacheLineitem(sc,sqlContext)

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
    Common.timeSqlQuery(query, "tpch/q1")
  }
}

object TPCHQuery5 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext   
    TPCHFiles.cacheCustomerHive(sqlContext)
    TPCHFiles.cacheOrdersHive(sqlContext)
    TPCHFiles.cacheLineitemHive(sqlContext)
    TPCHFiles.cacheSupplierHive(sqlContext)
    TPCHFiles.cacheNationHive(sqlContext)
    TPCHFiles.cacheRegionHive(sqlContext)

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
    TPCHFiles.cacheLineitem(sc,sqlContext)

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
    Common.timeSqlQuery(query, "tpch/q6")
  }
}

object TPCHQuery3 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext
    TPCHFiles.cacheLineitemHive(sqlContext)
    TPCHFiles.cacheCustomerHive(sqlContext)
    TPCHFiles.cacheOrdersHive(sqlContext)

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
    Common.timeHiveQuery(query, "tpch/q3")
  }
}

object TPCHQuery11 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext
    TPCHFiles.cachePartsuppHive(sqlContext)
    TPCHFiles.cacheSupplierHive(sqlContext)
    TPCHFiles.cacheNationHive(sqlContext)

    // Query with timing
    val query = """
      |select
      |  ps.ps_partkey,
      |  sum(ps.ps_supplycost * ps.ps_availqty) 
      |from
      |  partsupp as ps
      |  join supplier as s on ps.ps_suppkey = s.s_suppkey
      |  join nation as n on s.s_nationkey = n.n_nationkey
      |where
      |  n.n_name = 'GERMANY'
      |group by
      |  ps.ps_partkey having
      |    sum(ps2.ps_supplycost * ps2.ps_availqty) > (
      |      select
      |        sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
      |      from
      |        partsupp as ps2
      |        join supplier as s2 on ps2.ps_suppkey = s2.s_suppkey
      |        join nation as n2 on s_nationkey = n_nationkey
      |      where
      |        n2.n_name = 'GERMANY'
      |    )  
    """.stripMargin
    Common.timeHiveQuery(query, "tpch/q11")
  }
}
object TPCHQuery18 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext
    TPCHFiles.cacheLineitemHive(sqlContext)
    TPCHFiles.cacheCustomerHive(sqlContext)
    TPCHFiles.cacheOrdersHive(sqlContext)

    // Query with timing
    val query = """
    | select 
    |          c_name,
    |          c_custkey,
    |          o_orderkey,
    |          o_orderdate,
    |          o_totalprice,
    |          sum(l_quantity)
    |  from
    |          customer,
    |          orders,
    |          lineitem
    |  where
    |          o_orderkey in (
    |                  select
    |                          l_orderkey
    |                  from
    |                          lineitem
    |                  group by
    |                          l_orderkey having
    |                                  sum(l_quantity) > 300
    |          )
    |          and c_custkey = o_custkey
    |          and o_orderkey = l_orderkey
    |  group by
    |          c_name,
    |          c_custkey,
    |          o_orderkey,
    |          o_orderdate,
    |          o_totalprice
    |    
    """.stripMargin
    Common.timeHiveQuery(query, "tpch/q18")
  }
}

object TPCHQuery22 {
  def main(args: Array[String]) {
    // Common
    val sc = Common.sc
    val sqlContext = Common.hiveContext
    TPCHFiles.cacheCustomerHive(sqlContext)
    TPCHFiles.cacheOrdersHive(sqlContext)

    // Query with timing
    val query = """
      | select
      |         cntrycode,
      |         count(*) as numcust,
      |         sum(c_acctbal) as totacctbal
      | from
      |         (
      |                 select
      |                         substring(c_phone,1,2) as cntrycode,
      |                         c_acctbal
      |                 from
      |                         customer
      |                 where
      |                         substring(c_phone, 1, 2) in
      |                                 ('13', '31', '23', '29', '30', '18', '17')
      |                         and c_acctbal > (
      |                                 select
      |                                         avg(c_acctbal)
      |                                 from
      |                                         customer
      |                                 where
      |                                         c_acctbal > 0.00
      |                                         and substring(c_phone, 1, 2) in
      |                                                 ('13', '31', '23', '29', '30', '18', '17')
      |                         )
      |                         and not exists (
      |                                 select
      |                                         *
      |                                 from
      |                                         orders
      |                                 where
      |                                         o_custkey = c_custkey
      |                         )
      |         ) as custsale
      | group by
      |         cntrycode
    """.stripMargin
    Common.timeHiveQuery(query, "tpch/q22")
  }
}
