package edu.jhu.cs.damsl.k3.ml

import edu.jhu.cs.damsl.k3.common.MLDeployment
import scala.math._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import org.apache.flink.util._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import edu.jhu.cs.damsl.k3.common.MLDeployment

object SGD {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    val env = ExecutionEnvironment.getExecutionEnvironment

    val points: DataSet[Point] = getPointDataSet(env)    
    val parameters : DataSet[Array[Double]] = env.fromCollection(Seq(Array.fill[Double](dimensions)(0.0)))

    val finalParameters = parameters.iterate(numIterations) { currentParams =>
      points.reduceGroup(new ComputeGradient).withBroadcastSet(currentParams, "params")
    }

    finalParameters.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala SGD"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }
  
  private var dimensions: Int = 33
  private var stepSize : Double = 0.1
  private var lambda : Double = 0.1  
  private var numIterations: Int = 10
  
  private var deployment : MLDeployment = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 3) {
      deployment = new MLDeployment(args(1), args(0))
      numIterations = Integer.parseInt(args(2))
      if ( args.length == 4 ) { dimensions = Integer.parseInt(args(3)) }
      true
    }
    else {
      System.out.println("  Usage: SGD <result path> <scale factor> <num iterations> [dimensionality]")
      false
    }
  }

  def getPointDataSet(env: ExecutionEnvironment) : DataSet[Point] = {
    env.readCsvFile[Tuple1[String]](
      deployment.pointsPath(deployment.scaleFactor))
      .map {s => new Point(s._1.split(",").map {x => x.toDouble}) }
  }

  /**
   * A n-dimensional point.
   */
  class Point(var elems: Array[Double]) extends Serializable {
    def this(dims : Int) {
      this(new Array[Double](dims))
    }

    def this() {
      this(dimensions)
    }
    
    override def toString: String = {
      elems.mkString(" ")
    }
  }
  
  final class ComputeGradient extends RichGroupReduceFunction[Point, Array[Double]]() {
    private var params: Traversable[Array[Double]] = null

    override def open(parameters: Configuration) {
      params = getRuntimeContext.getBroadcastVariable[Array[Double]]("params").asScala
    }

    // Reduce now receives points that act as partial aggregates.
    override def reduce(in: java.lang.Iterable[Point], out: Collector[Array[Double]]) = {
      var paramAndCnt = Array.fill[Double](dimensions)(0)
      for (r <- in.asScala) {
        val point = r.elems.init
        val cnt = r.elems.last
        val newParams = add(paramAndCnt.init, point)
        val newCnt = paramAndCnt.last + cnt
        for (i <- 0 until dimensions-1) {
          paramAndCnt(i) = newParams(i)
        }
        paramAndCnt(dimensions-1) = newCnt
      }
      
      for (i <- 0 until dimensions-1) {
        paramAndCnt(i) = paramAndCnt(i) / paramAndCnt.last
      }
      out.collect(paramAndCnt)
    }
    
    override def combine(in: java.lang.Iterable[Point], out: Collector[Point]) = {
      var paramAndCnt : Array[Double] = Array.fill[Double](dimensions)(0)
      for (i <- 0 until dimensions-1){
        paramAndCnt(i) = params.head(i)
      }
      
      for (r <- in.asScala) {
        val class_label = r.elems.last
        val point = r.elems.init

        val lparams = paramAndCnt.init
        val cnt = paramAndCnt.last

        val flag = class_label * dot_product(lparams, point)
        val u = if ( flag > 1 ) { scale(lparams, lambda) }
                else { sub(scale(lparams, lambda), scale(point, class_label)) }
     
        val nlparams = sub(lparams, scale(u, stepSize))
        for (i <- 0 until nlparams.length) { paramAndCnt(i) = nlparams(i) }
        paramAndCnt(dimensions-1) += 1
      }
      out.collect(new Point(paramAndCnt))
    }

    def dot_product(a: Array[Double], b: Array[Double]) : Double = {
      a.zip(b).map(x => x._1 * x._2).reduce(_ + _)
    }
    
    def scale(x: Array[Double], a: Double) = {
      x.map(y => y * a)
    }
    
    def sub(a: Array[Double], b: Array[Double]) = {
      a.zip(b).map(i => i._1 - i._2)
    }

    def add(a: Array[Double], b: Array[Double]) = {
      a.zip(b).map(i => i._1 + i._2)
    }
  }
}
