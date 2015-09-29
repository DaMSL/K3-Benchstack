package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._

object TPCHQuery22 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
  }
  
  private def parseParameters(args: Array[String]): Boolean = {
    true
  }
}