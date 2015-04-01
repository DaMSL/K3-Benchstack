import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import common._

import org.apache.spark.graphx._


object PageRank {

  def main(args: Array[String]) = {
    val sc = Common.sc
    print("Loading graph")
    val graph = GraphLoader.edgeListFile(sc, "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/twitter_edgelist").cache()

    print("Starting pagerank")
    val start = System.currentTimeMillis
    graph.staticPageRank(1, .15)
    val end = System.currentTimeMillis
    print("Elapsed: " + (end - start).toString)

  }


}

