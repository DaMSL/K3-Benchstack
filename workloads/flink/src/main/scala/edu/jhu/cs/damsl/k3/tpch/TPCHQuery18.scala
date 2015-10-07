package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.core.fs.FileSystem.WriteMode

object TPCHQuery18 {
  type LO = (Long, Long, Double, String, Double)
  type LOC = (LO, Customer)
  type ResultGroup = (String, Long, Long, String, Double)
  type Result = (ResultGroup, Double)
  
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val lineitemHaving = getLineitemDataSet(env)
      .groupBy(0)
      .reduceGroup(new GroupReduceFunction[Lineitem,(Long,Double)] {
        override def reduce(in: java.lang.Iterable[Lineitem], out: Collector[(Long,Double)]) = {
          var ok : Option[Long] = None
          var sum_qty : Double = 0.0
          for (r <- in.asScala) {
            if ( ok.isEmpty ) { ok = Some(r.l_orderkey) }
            sum_qty += r.l_quantity
          }
          if ( ok.isDefined ) { out.collect( (ok.get, sum_qty) ) }
        }
      })
      .filter(lq => lq._2 > 300)
      
    val lineitemOrders = lineitemHaving.join(getOrdersDataSet(env))
      .where(l => l._1).equalTo(o => o.o_orderkey)
      .apply( (l,o) => (o.o_orderkey, o.o_custkey, o.o_totalprice, o.o_orderdate, l._2) )

    val lineitemOrdersCustomer = lineitemOrders.join(getCustomerDataSet(env))
      .where(lo => lo._2).equalTo(c => c.c_custkey)
      .groupBy[ResultGroup]( (loc:LOC) => (loc._2.c_name, loc._2.c_custkey, loc._1._1, loc._1._4, loc._1._3) )
      .reduceGroup( new GroupReduceFunction[LOC,Result] {
        override def reduce(in: java.lang.Iterable[LOC], out: Collector[Result]) =
        {
          var group : Option[ResultGroup] = None
          var sum_qty : Double = 0
          for ((lo,c) <- in.asScala) {
            if ( group.isEmpty ) { group = Some(c.c_name, c.c_custkey, lo._1, lo._4, lo._3) }
            sum_qty += lo._5
          }
          if ( group.isDefined ) { out.collect( (group.get, sum_qty) ) }
        }
      })

    lineitemOrdersCustomer.writeAsText(outputPath, WriteMode.OVERWRITE)
    
    val jobname = "Scala TPCH Q18"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }
  
  case class Lineitem(l_orderkey : Long,
                      l_quantity : Double )

  case class Customer(c_custkey : Long,
                      c_name    : String )

  case class Orders(o_orderkey   : Long,
                    o_custkey    : Long,
                    o_totalprice : Double,
                    o_orderdate  : String )

  private var lineitemPath: String = null
  private var customerPath: String = null
  private var ordersPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath   = args(2)
      outputPath   = args(3)
      true
    } else {
      System.err.println(
          " Usage: TPCHQuery18 <lineitem-csv path> <customer-csv path>" + 
                              "<orders-csv path> <result path>")
      false
    }
  }
  
  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        lineitemPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 4) )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
        customerPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1) )
  }
  
  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Orders] = {
    env.readCsvFile[Orders](
        ordersPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 3, 4) )
  }

}