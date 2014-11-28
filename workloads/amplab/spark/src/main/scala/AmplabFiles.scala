import org.apache.spark.SparkContext
import amplab.schema._
import org.apache.spark.rdd.RDD

package amplab.files {

  object AmplabFiles {
    val rankingsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/rankings/rankings"

    def getRankings(sc: SparkContext) = {
      val csv  = sc.textFile(rankingsPath).map(_.split(","))
      csv.map(r => Ranking(r(0),r(1).toInt,r(2).toInt))
    }

    val uservisitsPath = "hdfs://qp-hm1.damsl.cs.jhu.edu:54310/amplab/uservisits/uservisits"
    
    def getUservisits(sc: SparkContext) = {
      val csv = sc.textFile(uservisitsPath).map(_.split(","))
      csv.map({r => UserVisit(
          r(0),
          r(1),
          r(2),
          r(3).toDouble,
          r(4),
          r(5),
          r(6),
          r(7),
          r(8).toInt)
       })
    }

  }

}
