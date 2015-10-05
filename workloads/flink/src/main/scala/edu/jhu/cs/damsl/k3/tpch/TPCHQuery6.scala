package edu.jhu.cs.damsl.k3.tpch

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object TPCHQuery6 {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    
    // set filter date
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val dateGEQ    = dateFormat.parse("1994-01-01")
    val dateLT     = dateFormat.parse("1995-01-01")

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // scan lineitems, eval predicates
    val lineitems = getLineitemDataSet(env).filter(l => {
      val d = dateFormat.parse(l.l_shipdate) 
      val datePred = (d.after(dateGEQ) || d.equals(dateGEQ)) && d.before(dateLT)
      val discPred = 0.05 <= l.l_discount && l.l_discount <= 0.07
      datePred && discPred && l.l_quantity < 24 })

    // sum(l_extendedprice * l_discount)
    val result = lineitems.map(l => Tuple1(l.l_extendedprice * l.l_discount)).sum(0)

    // emit result at every taskmanager
    result.writeAsText(outputPath, WriteMode.OVERWRITE)
    //result.print()

    // execute program
    env.execute("Scala TPCH Q6")
  }
  
  case class Lineitem(l_quantity      : Double,
                      l_extendedprice : Double,
                      l_discount      : Double,
                      l_shipdate      : String )

  private var lineitemPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      lineitemPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.err.println("Usage: TPCHQuery6 <lineitem-csv path> <result path>")
      false
    }
  }
  
  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
        lineitemPath,
        fieldDelimiter = "|",
        includedFields = Array(4, 5, 6, 10) )
  }

}