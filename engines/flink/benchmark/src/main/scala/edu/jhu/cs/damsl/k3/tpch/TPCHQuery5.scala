package edu.jhu.cs.damsl.k3.tpch

import edu.jhu.cs.damsl.k3.common.TPCHDeployment

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
    
    val rn = getQ5RegionDataSet(env)
                .filter(r => r.r_name == "ASIA")
                .join(getQ5NationDataSet(env)).where("r_regionkey").equalTo("n_regionkey")

    val suppliersInAsia = getQ5SupplierDataSet(env).filter(new SupplierFilter()).withBroadcastSet(rn, "RNJoin")
    
    // TODO: broadcast supplier into lineitem as with K3?
    val ls = getQ5LineitemDataSet(env)
              .map(l => (l.l_orderkey, l.l_suppkey, l.l_extendedprice * (1 - l.l_discount)) )
              .join(suppliersInAsia).where(1).equalTo(0)
              .apply((l,s,out:Collector[LS]) => out.collect((l._1, s.s_nationkey, l._3)))
                // l_orderkey, l_nationkey, l_epd triples
              
    val orderCustKeys = getQ5OrdersDataSet(env).filter(o => {
          val d = dateFormat.parse(o.o_orderdate) 
          (d.after(dateGEQ) || d.equals(dateGEQ)) && d.before(dateLT) 
         })
         .map(o => (o.o_custkey, o.o_orderkey))

    val co = getQ5CustomerDataSet(env)
              .filter(new CustomerFilter()).withBroadcastSet(rn, "RNJoin")
              .join(orderCustKeys).where(0).equalTo(0)
              .map(co => (co._2._2, co._1.c_nationkey))
                // o_orderkey, c_nationkey pairs

    val lsco = ls.join(co).where(0,1).equalTo(0,1)
                 .apply((ls,co,out:Collector[LS]) => out.collect(ls))
                 .groupBy(1)
                 .reduceGroup(new LSCOReducer()).withBroadcastSet(rn, "RNJoin")
      
    lsco.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala TPCH Q5"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
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

  private var deployment : TPCHDeployment = null
  
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      deployment = new TPCHDeployment(args(0), args(1))
      true
    } else {
      System.err.println("Usage: TPCHQuery5 <scale-factor> <result path>")
      false
    }
  }

  def getQ5LineitemDataSet(env: ExecutionEnvironment) : DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        deployment.lineitemPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 2, 5, 6) )
  }

  def getQ5CustomerDataSet(env: ExecutionEnvironment) : DataSet[Customer] = {
    env.readCsvFile[Customer](
        deployment.customerPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 3) )
  }
  
  def getQ5OrdersDataSet(env: ExecutionEnvironment) : DataSet[Orders] = {
    env.readCsvFile[Orders](
        deployment.ordersPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 4) )
  }

  def getQ5SupplierDataSet(env: ExecutionEnvironment) : DataSet[Supplier] = {
    env.readCsvFile[Supplier](
        deployment.supplierPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 3) )
  }

  def getQ5NationDataSet(env: ExecutionEnvironment) : DataSet[Nation] = {
    env.readCsvFile[Nation](
        deployment.nationPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 2) )
  }
		
  def getQ5RegionDataSet(env: ExecutionEnvironment) : DataSet[Region] = {
    env.readCsvFile[Region](
        deployment.regionPath(deployment.scaleFactor),
        fieldDelimiter = "|",
        includedFields = Array(0, 1) )
  }
}