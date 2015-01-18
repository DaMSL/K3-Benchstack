import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.hive.HiveContext

package common {

object Common {
    val conf = new SparkConf()
             .setMaster("spark://qp-hm1.damsl.cs.jhu.edu:7077")
             .setAppName("Queries")
             .setSparkHome("/software/spark-1.1.0-bin-hadoop2.4")
             .set("spark.executor.memory", "65g")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)


    def printOutput(result: SchemaRDD, time: Long)  = {
        println("====== START PLAN ---->>")
        println(result.queryExecution.executedPlan.toString())
        println("<<---- END PLAN   ======")
        println("Num Results: " + result.count )
        println("Elapsed: " + time.toString)
    }

    def timeSqlQuery(query: String, outfile: String, printPlan: Boolean = false) = {
      // Start timer
      var start = System.currentTimeMillis

      // Run Query
      val result =  sqlContext.sql(query)

      // Display Executed Query Plan
//      if (printPlan) {
//        println("===========================================")
//        println(result.queryExecution.executedPlan.toString())
//        println("===========================================")
//      }

      // Force evaluation with .count
//      println("Num Results: " + result.count )

      // Stop timer
      var end = System.currentTimeMillis
//      println("Elapsed: " + (end - start).toString)

      printOutput(result, (end - start))

      // TODO: Force overwrite or change filename (this throws an error) Save results to HDFS
//      val path = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/spark/" + outfile
//      result.saveAsTextFile(path)
    }
    

    def timeHiveQuery(query: String, outfile: String, printPlan: Boolean = false) = {
      // Start timer
      var start = System.currentTimeMillis

      // Run Query
      val result =  hiveContext.sql(query)

      // Force evaluation with .count
//      println("Num Results: " + result.count )

      // Stop timer
      var end = System.currentTimeMillis
//      println("Elapsed: " + (end - start).toString)

      printOutput(result, (end - start))

      // TODO: Force overwrite or change filename (this throws an error) Save results to HDFS
//      val path = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/results/spark/" + outfile
//      result.saveAsTextFile(path)
    }
}

}
