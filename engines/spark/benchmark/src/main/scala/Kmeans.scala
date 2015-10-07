import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import common._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.squaredDistance

object KMeans {
  def main(args: Array[String]) = {
    val sc = Common.sc
    val sf = args(0)
    
    /* Parse input data, cache, force evaluation */
    val path = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/sgd_single_file/$sf/sgd$sf"
    val rawData = sc.textFile(path)
    val parsedData = rawData.map(s => s.split(',').map(_.toDouble) ).cache()
    println(parsedData.count().toString + " Points Cached")

    /* KMeans config */
    val k = 5
    val numIters = 10
    var means = parsedData.take(k) // Initial Means

    val times : Array[Double] = new Array[Double](numIters)

    /* Main Loop: TODO should we include time between iterations? */
    (0 until numIters).foreach { i =>
      val start = System.currentTimeMillis

      /* Collect a sum and count for each Mean */
      val totalContribs = parsedData.map { v => 
        val i = closestMean(means, v)
        (i, (v, 1))
      }.reduceByKey(mergeContribs).collectAsMap()
     
      /* Compute new Means */ 
      (0 until k).foreach {m => 
        val (sum, count) = totalContribs(m)
        means(m) = scale( (1.0 / count), sum)
      }

      val end = System.currentTimeMillis
      times(i) = end - start
    }
   
    /* Summary */ 
    means.foreach {m =>
      println("Mean:")
      m.foreach(print)
      println("")
    }    

    var totalTime : Double = 0.0

    // TODO
    times.foreach { t =>
      println("Time: " + t.toString)
      totalTime += t
    }

    println("Elapsed: " + (totalTime / numIters).toString)
  }

  /* Vector Utils */
  def add(v1 : Array[Double], v2 : Array[Double]) : Array[Double] = {
    var result : Array[Double] = new Array[Double](v1.length)
    (0 until v1.length).foreach(i => result(i) = v1(i) + v2(i) )
    result 
  }

  def scale(c : Double, v : Array[Double]) : Array[Double] = {
    var result : Array[Double] = new Array[Double](v.length)
    (0 until v.length).foreach(i => result(i) = c * v(i))
    result 
  }

  def squaredDistance(v1 : Array[Double], v2: Array[Double]) : Double = {
    var sum : Double = 0.0
    (0 until v1.length).foreach(i => sum += v1(i) - v2(i) * v1(i) - v2(i))
    sum
  }

  def closestMean(means: Array[Array[Double]], point: Array[Double]) : Int = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    means.foreach { center =>
      var sqDist = squaredDistance(center, point)
      if (sqDist < bestDistance) {
        if (sqDist < bestDistance) {
          bestDistance = sqDist
          bestIndex = i
        }
      }
      i += 1
    }
    bestIndex
  }

  def mergeContribs(t1: (Array[Double], Int), t2: (Array[Double], Int)) : (Array[Double], Int) = {
    ( add(t1._1, t2._1), t1._2 + t2._2 )
  }
}
