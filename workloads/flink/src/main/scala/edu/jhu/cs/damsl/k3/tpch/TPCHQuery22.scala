package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object TPCHQuery22 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    
    // Compute average c_acctbal for broadcasting.
    val custSumCnt = getCustomerDataSet(env).filter( (c:Customer) => {
        val codes = List("13", "31", "23", "29", "30", "18", "17") 
        c.c_acctbal > 0.0 && codes.contains(c.c_phone.substring(0, 2)) 
      }).reduceGroup(new GroupReduceFunction[Customer,(Double,Long)] {
        override def reduce(in: java.lang.Iterable[Customer], out:Collector[(Double,Long)]) = {
          var sum_ab : Double = 0.0
          var cnt : Long = 0
          for (r <- in.asScala) {
            sum_ab += r.c_acctbal
            cnt += 1
          }
          out.collect( (sum_ab, cnt) )
        }
      })
      
    val sumCnt = custSumCnt.collect().head
    val avgBal = sumCnt._1 / sumCnt._2
    val avgBroadcast = env.fromElements(avgBal)

    // Filter customers (using broadcasted c_acctbal and orderkeys) and aggregate.
    val result = getCustomerDataSet(env)
      .filter(new RichFilterFunction[Customer]() {
        var globalAvg : Traversable[Double] = null
        var avgAccBal : Double = 0.0
              
        override def open(config: Configuration) = {
          globalAvg = getRuntimeContext().getBroadcastVariable[Double]("avgCAcctbal").asScala
          avgAccBal = globalAvg.head
        }
        
        override def filter(c:Customer) = {
          val codes = List("13", "31", "23", "29", "30", "18", "17")
          c.c_acctbal > avgAccBal && codes.contains(c.c_phone.substring(0,2)) 
        }
      })
      .withBroadcastSet(avgBroadcast, "avgCAcctbal")
      .coGroup(getOrdersDataSet(env)).where(0).equalTo(0)
      .apply( (cs,os,out:Collector[Customer]) => {
        if ( os.isEmpty ) { for (c <- cs) { out.collect(c) } }  
      })
      .map(c => (c.c_phone.substring(0,2), c.c_acctbal))
      .groupBy(0)
      .reduceGroup(new GroupReduceFunction[(String, Double), (String, Double, Long)] {
        override def reduce(in: java.lang.Iterable[(String, Double)], out: Collector[(String, Double, Long)]) = {
          var key : Option[String] = None
          var sum_bal : Double = 0.0
          var cnt : Long = 0
          for (r <- in.asScala) {
            if ( key.isEmpty ) { key = Some(r._1) }
            sum_bal += r._2
            cnt += 1
          }
          if ( key.isDefined ) { out.collect( (key.get, sum_bal, cnt) ) }
        }
      })
    
    result.writeAsText(outputPath, WriteMode.OVERWRITE)
    env.execute("Scala TPCH Q22")
  }

  case class Customer( c_custkey : Long,
                       c_phone   : String,
                       c_acctbal : Double )

  case class Orders( o_custkey : Long )

  private var customerPath: String = null
  private var ordersPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      customerPath = args(0)
      ordersPath   = args(1)
      outputPath   = args(2)
      true
    } else {
      System.err.println(
          " Usage: TPCHQuery22 <customer-csv path> <orders-csv path> <result path>")
      false
    }

  }
  
  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
        customerPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 4, 5) )
  }
  
  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Orders] = {
    env.readCsvFile[Orders](
        ordersPath,
        fieldDelimiter = "|",
        includedFields = Array(1) )
  }

}