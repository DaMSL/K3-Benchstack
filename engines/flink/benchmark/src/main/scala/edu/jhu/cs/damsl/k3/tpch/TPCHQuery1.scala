package edu.jhu.cs.damsl.k3.tpch

import edu.jhu.cs.damsl.k3.common.TPCHDeployment

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.core.fs.FileSystem.WriteMode

object TPCHQuery1 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set filter date
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse("1998-09-02")

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // scan lineitems, eval l_shipdate <= date '1998-09-02'
    val lineitems = getQ1LineitemDataSet(env).filter(l => {
      val d = dateFormat.parse(l.l_shipdate)
      d.before(date) || d.equals(date) })

    // group by order and aggregate revenue
    val result : DataSet[Q1Result] = lineitems.groupBy("l_returnflag", "l_linestatus")
                                              .reduceGroup(new Q1GroupReduceFunction())

    // emit result at every taskmanager
    result.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

    // execute program
    val jobname = "Scala TPCH Q1"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }

  class Q1GroupReduceFunction extends GroupReduceFunction[Lineitem,Q1Result] {
    override def reduce(in: java.lang.Iterable[Lineitem], out: Collector[Q1Result]) : Unit = {
      var acc : Q1Result = new Q1Result()
      for (r <- in.asScala) {
        if ( acc.l_returnflag.isEmpty() || acc.l_linestatus.isEmpty() ) {
          acc.l_returnflag = r.l_returnflag
          acc.l_linestatus = r.l_linestatus
        }
        acc.sum_qty        += r.l_quantity
        acc.sum_base_price += r.l_extendedprice
        acc.sum_disc_price += (r.l_extendedprice * (1 - r.l_discount))
        acc.sum_charge     += (r.l_extendedprice * (1 - r.l_discount) * (1 + r.l_tax))
        acc.avg_qty        += r.l_quantity
        acc.avg_price      += r.l_extendedprice
        acc.avg_disc       += r.l_discount
        acc.count_order    += 1
      }
      acc.avg_qty   = acc.avg_qty   / acc.count_order
      acc.avg_price = acc.avg_price / acc.count_order
      acc.avg_disc  = acc.avg_disc  / acc.count_order
      out.collect(acc)
    }
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Lineitem(l_quantity      : Double,
                      l_extendedprice : Double,
                      l_discount      : Double,
                      l_tax           : Double,
                      l_returnflag    : String,
                      l_linestatus    : String,
                      l_shipdate      : String )

  class Q1Result(var l_returnflag    : String,
                 var l_linestatus    : String,
                 var sum_qty         : Double,
                 var sum_base_price  : Double,
                 var sum_disc_price  : Double,
                 var sum_charge      : Double,
                 var avg_qty         : Double,
                 var avg_price       : Double,
                 var avg_disc        : Double,
                 var count_order     : Int)
  {
    def this() = this("", "", 0.0,0.0,0.0,0.0,0.0,0.0,0.0,0)

    override def toString() = {
      l_returnflag + "|" + l_linestatus + "|" + sum_qty + "|" + sum_base_price + "|" + sum_disc_price + "|" + sum_charge + "|" + avg_qty + "|" + avg_price + "|" + avg_disc + "|" + count_order
    }
  }

  private var deployment : TPCHDeployment = null
  
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      deployment = new TPCHDeployment(args(1), args(0))
      true
    } else {
      System.err.println("Usage: TPCHQuery1 <result path> <scale-factor>" )
      false
    }
  }
  
  def getQ1LineitemDataSet(env: ExecutionEnvironment) : DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        deployment.lineitemPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(4, 5, 6, 7, 8, 9, 10) )
  }
}
