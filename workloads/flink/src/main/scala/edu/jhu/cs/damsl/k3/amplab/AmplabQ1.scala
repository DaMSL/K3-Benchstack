package edu.jhu.cs.damsl.k3.amplab

import org.apache.flink.api.scala._

object AmplabQ1 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
  }
  
  private def parseParameters(args: Array[String]): Boolean = {
    true
  }
}