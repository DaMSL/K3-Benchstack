package edu.jhu.cs.damsl.k3.graph

import scala.math._
import scala.collection.immutable.HashMap
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/*
 * This PageRank implementation is based on Flink's examples
 * 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-scala-examples/ \
 *   src/main/scala/org/apache/flink/examples/scala/graph/PageRankBasic.scala 
 */

object PageRank {
  
  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001

  private final val RemainderVertex : Int = -1

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val vertices = getAdjVertexDataSet(env)
    val ranks = env.fromCollection(Seq((RemainderVertex, 0.0)))
                   .union(vertices.map(x => (x.id, 0.0)))

    val parameters = (vertices.count(), (1 - DAMPENING_FACTOR) / vertices.count())
    val broadcastParams = env.fromCollection(Seq(parameters))

    val finalRanks = ranks.iterate(numIterations) { currentRanks =>
      val nextRanks = currentRanks.join(vertices).where(0).equalTo(0)
        .apply(new PRJoin()).withBroadcastSet(broadcastParams, "parameters")
        .groupBy(0).sum(1) 

      val remainder = nextRanks.filter(vr => vr._1 == RemainderVertex)
      nextRanks.map(new RemainderMap()).withBroadcastSet(remainder, "remainder")
    }

    finalRanks.writeAsText(outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala PageRank"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }

  class PRJoin extends RichFlatJoinFunction[(Int,Double), AdjVertex, (Int,Double)]() {
    var params : Traversable[(Long, Double)] = null
    var num_vertices : Long = 0
    var default_contrib : Double = 0
    
    override def open(config : Configuration) = {
      params = getRuntimeContext().getBroadcastVariable[(Long, Double)]("parameters").asScala
      num_vertices = params.head._1
      default_contrib = params.head._2
    }
    
    override def join(r: (Int,Double), v: AdjVertex, out: Collector[(Int,Double)]) = {
      val out_degree = v.neighbors.length
      val damped_value = default_contrib + DAMPENING_FACTOR * r._1
      if ( out_degree == 0 ) {
        val out_contrib = damped_value / num_vertices
        out.collect(RemainderVertex, out_contrib)
      } else {
        val out_contrib = damped_value / out_degree
        v.neighbors.foreach { n => out.collect(n, out_contrib) } 
      }
    }
  }
  
  class RemainderMap extends RichMapFunction[(Int,Double), (Int,Double)]() {
    var remainder_contrib : Double = 0.0
    override def open(config: Configuration) = {
      remainder_contrib = getRuntimeContext().getBroadcastVariable[(Long, Double)]("remainder").asScala.head._2
    }
    
    override def map(vr: (Int,Double)) = {
      if ( vr._1 == RemainderVertex ) {
        (RemainderVertex, 0.0)
      } else {
        (vr._1, vr._2 + remainder_contrib)
      }
    }
  }
  
  /*
   * An adjacency list vertex.
   */
  class AdjVertex(var id: Int, var neighbors: Array[Int]) extends Serializable {
    def this() {
      this(-1, new Array[Int](0))
    }
    
    def this(elems : Array[Int]) {
      this(elems.head, elems.tail)
    }
    
    override def toString: String = {
      id + " " + neighbors.mkString(" ")
    }
  }
  
  private var graphPath: String = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/twitter_edgelist"
  private var outputPath: String = null
  private var numIterations: Int = 10

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 2) {
      outputPath    = args(0)
      numIterations = Integer.parseInt(args(1))
      if ( args.length == 3 ) { graphPath = args(2) }
      true
    }
    else {
      System.out.println("  Usage: PageRank <result path> <num iterations> [graph path]")
      false
    }
  }

  private def getAdjVertexDataSet(env: ExecutionEnvironment): DataSet[AdjVertex] = {
    env.readCsvFile[AdjVertex](graphPath, fieldDelimiter = " ")
  }

}