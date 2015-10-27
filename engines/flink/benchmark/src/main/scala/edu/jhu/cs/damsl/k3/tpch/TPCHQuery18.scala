package edu.jhu.cs.damsl.k3.tpch

import edu.jhu.cs.damsl.k3.common.TPCHDeployment

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

    val lineitemHaving = getQ18LineitemDataSet(env)
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
      
    val lineitemOrders = lineitemHaving.join(getQ18OrdersDataSet(env))
      .where(l => l._1).equalTo(o => o.o_orderkey)
      .apply( (l,o) => (o.o_orderkey, o.o_custkey, o.o_totalprice, o.o_orderdate, l._2) )

    val lineitemOrdersCustomer = lineitemOrders.join(getQ18CustomerDataSet(env))
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

    lineitemOrdersCustomer.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)
    
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

  private var deployment : TPCHDeployment = null
  
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      deployment = new TPCHDeployment(args(1), args(0))
      true
    } else {
      System.err.println("Usage: TPCHQuery18 <result path> <scale-factor>")
      false
    }
  }  

  def getQ18LineitemDataSet(env: ExecutionEnvironment) : DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        deployment.lineitemPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 4) )
  }

  def getQ18CustomerDataSet(env: ExecutionEnvironment) : DataSet[Customer] = {
    env.readCsvFile[Customer](
        deployment.customerPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 1) )
  }
  
  def getQ18OrdersDataSet(env: ExecutionEnvironment) : DataSet[Orders] = {
    env.readCsvFile[Orders](
        deployment.ordersPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 3, 4) )
  }
}
