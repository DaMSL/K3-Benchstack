package edu.jhu.cs.damsl.k3.ml

import scala.math._
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

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

    val points: DataSet[Point] = getPointDataSet(env)
    val centroids: DataSet[Centroid] = getCentroidDataSet(env)

    val finalCentroids = centroids.iterate(numIterations) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2.add(p2._2), p1._3 + p2._3) }
        .map { x => new Centroid(x._1, x._2.div(x._3)) }
      newCentroids
    }

    finalCentroids.writeAsText(outputPath, WriteMode.OVERWRITE)
    env.execute("Scala KMeans")
  }
  
  private var dimensions: Int = 33
  private var pointsPath: String = null
  private var centersPath: String = null
  private var outputPath: String = null
  private var numIterations: Int = 10

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length >= 4) {
      pointsPath    = args(0)
      centersPath   = args(1)
      outputPath    = args(2)
      numIterations = Integer.parseInt(args(3))
      if ( args.length == 5 ) { dimensions = Integer.parseInt(args(4)) }
      true
    }
    else {
      System.out.println("  Usage: KMeans <points path> <centers path> <result path> <num iterations> [dimensionality]")
      false
    }
  }

  private def getPointDataSet(env: ExecutionEnvironment): DataSet[Point] = {
    env.readCsvFile[Point](pointsPath, fieldDelimiter = " ")
  }

  private def getCentroidDataSet(env: ExecutionEnvironment): DataSet[Centroid] = {
    env.readCsvFile[Centroid](centersPath, fieldDelimiter = " ")
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
      this(0, new Array[Double](dims))
    }

    def this(id: Int, p: Point) {
      this(id, p.elems)
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