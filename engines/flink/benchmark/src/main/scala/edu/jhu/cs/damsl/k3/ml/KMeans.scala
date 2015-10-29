package edu.jhu.cs.damsl.k3.ml

import edu.jhu.cs.damsl.k3.common.MLDeployment
import scala.math._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import edu.jhu.cs.damsl.k3.common.MLDeployment

/*
 * This k-means implementation is based on Flink's examples, generalizing it
 * to an n-dimensional dataset.
 * 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-scala-examples/ \
 * 	 src/main/scala/org/apache/flink/examples/scala/clustering/KMeans.scala
 * 
 */
object KMeans {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val k : Int = 5

    val points: DataSet[Point] = getPointDataSet(env)
    val firstK: Seq[Point] = points.first(k).collect()

    val initialCentroids = new Array[Centroid](k)
    for (i <- 0 until k) {
      initialCentroids(i) = new Centroid(i, firstK(i))
    }

    val centroids: DataSet[Centroid] = env.fromCollection(initialCentroids)

    val finalCentroids = centroids.iterate(numIterations) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }
        .map { x => new Centroid(x._1, x._2.div(x._3)) }
      newCentroids
    }

    finalCentroids.writeAsText(deployment.outputPath, WriteMode.OVERWRITE)

    val jobname = "Scala KMeans"
    val jobresult = env.execute(jobname)
    print(jobname + " time: " + jobresult.getNetRuntime)
    print(jobname + " plan:\n" + env.getExecutionPlan())
  }
  
  private var dimensions: Int = 33
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
      System.out.println("  Usage: KMeans <result path> <scale factor> <num iterations> [dimensionality]")
      false
    }
  }


  def getPointDataSet(env: ExecutionEnvironment) : DataSet[Point] = {
    env.readTextFile(deployment.pointsPath(deployment.scaleFactor))
       .map {s => new Point(s.split(",").map {x => x.toDouble}) }
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
    
    def add(other: Point): Point = {
      for (i <- 0 until Math.min(elems.length, other.elems.length)) {
        elems(i) += other.elems(i)
      }
      this
    }

    def div(other: Long): Point = {
      for (i <- 0 until elems.length) {
        elems(i) /= other
      }
      this
    }

    def euclideanDistance(other: Point): Double = {
      var sum : Double = 0.0
      for (i <- 0 until Math.min(elems.length, other.elems.length)) {
        sum += Math.pow(elems(i) - other.elems(i), 2)
      }
      Math.sqrt(sum)
    }

    def clear(): Unit = {
      for (i <- 0 until elems.length) {
        elems(i) = 0
      }
    }
    
    override def toString: String = {
      elems.mkString(" ")
    }
  }

  /**
   * An n-dimensional centroid.
   */
  class Centroid(var id: Int, elems: Array[Double]) extends Point(elems) {
    def this(dims : Int) {
      this(-1, new Array[Double](dims))
    }

    def this(id: Int, p: Point) {
      this(id, p.elems)
    }

    def this() {
      this(-1, new Array[Double](dimensions))
    }
    
    override def toString: String = {
      id + " " + super.toString
    }
  }

  /** Determines the closest cluster center for a data point. */
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }
  }
}
