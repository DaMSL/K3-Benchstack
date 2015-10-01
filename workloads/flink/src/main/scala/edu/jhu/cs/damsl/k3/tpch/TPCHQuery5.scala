package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._

object TPCHQuery5 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.execute("Scala TPCH Q5")
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
          " Usage: TPCHQuery18 <lineitem-csv path> <customer-csv path>" + 
                              "<orders-csv path> <result path>")
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