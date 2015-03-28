import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import common._

object SGD {
  
  def inPlaceScale(v : Array[Double], c : Double) = {
    (0 until v.length).foreach {i =>
      v(i) *= c
    }
  }
  
  def scale(v : Array[Double], c : Double) : Array[Double] = {
    var result = new Array[Double](v.length)
    (0 until v.length).foreach {i =>
      result(i) *= c
    }
    result
  }
  
  def add(v1 : Array[Double], v2 : Array[Double]) : Array[Double] = {
    var result : Array[Double] = new Array[Double](v1.length)
    (0 until v1.length).foreach(i => result(i) = v1(i) + v2(i) )
    result 
  }
  
  def sub(v1 : Array[Double], v2 : Array[Double]) : Array[Double] = {
    var result : Array[Double] = new Array[Double](v1.length)
    (0 until v1.length).foreach(i => result(i) = v1(i) - v2(i) )
    result 
  }

  def inPlaceSub(v : Array[Double], v2: Array[Double]) = {
    (0 until v.length).foreach {i =>
      v(i) -= v2(i)
    }
  }

  def dot(v1: Array[Double], v2: Array[Double]) = {
    var sum = 0.0
    (0 until v1.length).foreach {i =>
      sum += v1(i) * v2(i)
    }
    sum
  }

  def main(args: Array[String]) = {
    val sc = Common.sc
    val sf = args(0)
    val path = s"hdfs://qp-hm1.damsl.cs.jhu.edu:54310/sgd_single_file/$sf/sgd$sf"
    val rawData = sc.textFile(path)
    val intData = rawData.map(s => ( s.split(',').map(_.toDouble) )).cache()
    val parsedData = intData.map(v => (v.init, v.last)).cache()
    println(parsedData.count.toString + " rows parsed!")
    val lambda = 0.1
    val alpha = 0.1
    val dims = parsedData.take(1)(0)._1.length
    val iters = 10
    val times = new Array[Double](iters)

    println("DIMS: " + dims)
    var params = new Array[Double](dims)
    (0 until params.length).foreach(i => params(i) = .1)

   
    type LabeledPoint = (Array[Double], Double)
    def gradient(params: Array[Double], point: LabeledPoint) : Array[Double] = {
      val features = point._1
      val label    = point._2
      val flag = label * dot(params, features)
      if (flag > 1) {
        scale(params, lambda)
      } else {
        val v = scale(params, lambda)
        sub(v, scale(features, label))
      }
    }

    (0 until iters).foreach { i =>
      val start = System.currentTimeMillis
      val (sum, count) = parsedData.mapPartitions { ps => 
        for (point <- ps) {
            val update = scale(gradient(params, point), alpha)
            inPlaceSub(params, update)
        }

        val foo = for (j <- 0 until 1) yield {
          (params, 1)
        }

        foo.iterator 
      }.reduce(mergeContribs)
      val end = System.currentTimeMillis
 
      (0 until params.length).foreach(i => params(i) = sum(i) / count)

      times(i) = (end-start)
    }

      var s = 0.0
      var skip = true
      (0 until iters).foreach(i => 
        if (skip) {
          skip = false
        } else {
          s += times(i)
        }
      )
      val avg = s / iters
      println("Elapsed: " + avg.toString)

    }

  def mergeContribs(t1: (Array[Double], Int), t2: (Array[Double], Int)) : (Array[Double], Int) = {
    ( add(t1._1, t2._1), t1._2 + t2._2 )
  }

  }
