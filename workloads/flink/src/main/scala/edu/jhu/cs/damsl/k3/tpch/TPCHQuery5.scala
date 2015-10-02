package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object TPCHQuery5 {
  type Q5Result = (String, Double)
  type RN = (Region, Nation)
  type LS = (Long, Long, Double)
  
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val dateGEQ    = dateFormat.parse("1994-01-01")
    val dateLT     = dateFormat.parse("1995-01-01")

    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val rn = getRegionDataSet(env)
                .filter(r => r.r_name == "ASIA")
                .join(getNationDataSet(env)).where("r_regionkey").equalTo("n_regionkey")

    val suppliersInAsia = getSupplierDataSet(env).filter(new SupplierFilter()).withBroadcastSet(rn, "RNJoin")
    
    // TODO: broadcast supplier into lineitem as with K3?
    val ls = getLineitemDataSet(env)
              .map(l => (l.l_orderkey, l.l_suppkey, l.l_extendedprice * (1 - l.l_discount)) )
              .join(suppliersInAsia).where(1).equalTo(0)
              .apply((l,s,out:Collector[LS]) => out.collect((l._1, s.s_nationkey, l._3)))
                // l_orderkey, l_nationkey, l_epd triples
              
    val orderCustKeys = getOrdersDataSet(env).filter(o => {
          val d = dateFormat.parse(o.o_orderdate) 
          (d.after(dateGEQ) || d.equals(dateGEQ)) && d.before(dateLT) 
         })
         .map(o => (o.o_custkey, o.o_orderkey))

    val co = getCustomerDataSet(env)
              .filter(new CustomerFilter()).withBroadcastSet(rn, "RNJoin")
              .join(orderCustKeys).where(0).equalTo(0)
              .map(co => (co._2._2, co._1.c_nationkey))
                // o_orderkey, c_nationkey pairs

    val lsco = ls.join(co).where(0,1).equalTo(0,1)
                 .apply((ls,co,out:Collector[LS]) => out.collect(ls))
                 .groupBy(1)
                 .reduceGroup(new LSCOReducer()).withBroadcastSet(rn, "RNJoin")
      
    lsco.writeAsText(outputPath, WriteMode.OVERWRITE)
    env.execute("Scala TPCH Q5")
  }
  
  class CustomerFilter extends RichFilterFunction[Customer]() {
    var rn : Traversable[RN] = null
          
    override def open(config: Configuration) = {
      rn = getRuntimeContext().getBroadcastVariable[RN]("RNJoin").asScala
    }
    
    override def filter(c:Customer) = {
      rn.exists( x => x._2.n_nationkey == c.c_nationkey)
    }
  }

  class SupplierFilter extends RichFilterFunction[Supplier]() {
    var rn : Traversable[RN] = null
          
    override def open(config: Configuration) = {
      rn = getRuntimeContext().getBroadcastVariable[RN]("RNJoin").asScala
    }
    
    override def filter(s:Supplier) = {
      rn.exists( x => x._2.n_nationkey == s.s_nationkey)
    }
  }

  class LSCOReducer extends RichGroupReduceFunction[LS, Q5Result]() {
     var rn : Traversable[RN] = null

     override def open(config: Configuration) = {
       rn = getRuntimeContext().getBroadcastVariable[RN]("RNJoin").asScala 
     }
     
     override def reduce(in: java.lang.Iterable[LS], out: Collector[Q5Result]) = {
       var n_name : Option[String] = None
       var sum_epd : Double = 0.0

       for (r <- in.asScala) {
         if ( n_name.isEmpty ) {
           n_name = rn.find(x => x._2.n_nationkey == r._2) match {
             case Some(x) => Some(x._2.n_name)
             case None => None 
           }
         }
         sum_epd += r._3
       }
       if ( n_name.isDefined ) { out.collect( (n_name.get, sum_epd) ) }
     }
   }

  case class Lineitem(l_orderkey      : Long,
                      l_suppkey       : Long,
                      l_extendedprice : Double,
                      l_discount      : Double)

  case class Customer(c_custkey   : Long,
                      c_nationkey : Long)

  case class Orders(o_orderkey   : Long,
                    o_custkey    : Long,
                    o_orderdate  : String)

  case class Supplier(s_suppkey   : Long,
                      s_nationkey : Long)
  
  case class Nation(n_nationkey : Long,
                    n_name      : String,
                    n_regionkey : Long)
  
  case class Region(r_regionkey : Long,
                    r_name      : String)

  private var lineitemPath : String = null
  private var customerPath : String = null
  private var ordersPath   : String = null
  private var supplierPath : String = null
  private var nationPath   : String = null
  private var regionPath   : String = null
  private var outputPath   : String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 7) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath   = args(2)
      supplierPath = args(3)
      nationPath   = args(4)
      regionPath   = args(5)
      outputPath   = args(6)
      true
    } else {
      System.err.println(
          " Usage: TPCHQuery5 <lineitem-csv path> <customer-csv path> <orders-csv path> <supplier-csv path> <nation-csv path> <region-csv path> <result path>")
      false
    }
  }
  
  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        lineitemPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 2, 5, 6) )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
        customerPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 3) )
  }
  
  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Orders] = {
    env.readCsvFile[Orders](
        ordersPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 4) )
  }

  private def getSupplierDataSet(env: ExecutionEnvironment): DataSet[Supplier] = {
    env.readCsvFile[Supplier](
        supplierPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 3) )
  }

  private def getNationDataSet(env: ExecutionEnvironment): DataSet[Nation] = {
    env.readCsvFile[Nation](
        nationPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 2) )
  }
		
  private def getRegionDataSet(env: ExecutionEnvironment): DataSet[Region] = {
    env.readCsvFile[Region](
        regionPath,
        fieldDelimiter = "|",
        includedFields = Array(0, 1) )
  }
}