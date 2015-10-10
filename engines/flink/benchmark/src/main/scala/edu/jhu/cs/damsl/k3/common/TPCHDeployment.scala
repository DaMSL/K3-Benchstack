package edu.jhu.cs.damsl.k3.common

import org.apache.flink.api.scala._

class TPCHDeployment(var scaleFactor: String, var outputPath: String)
{
  def lineitemPath(sf: String) = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/lineitem/" }
  def partsuppPath(sf: String) = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/partsupp" }
  def nationPath(sf: String)   = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/nation" }
  def regionPath(sf: String)   = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/region" }
  def supplierPath(sf: String) = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/supplier" }
  def customerPath(sf: String) = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/customer" }
  def ordersPath(sf: String)   = { s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/tpch_fixed/$sf/orders" }

  def this() { this("10g", "/query") }
}