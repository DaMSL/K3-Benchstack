// Apache Flink implementation of AMPLAB Big-Data Benchmark: Query #1.
package edu.jhu.cs.damsl.flink.queries.amplab;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import edu.jhu.cs.damsl.flink.queries.amplab.types.*;

public class Q1 {
  public static void main(String[] args) throws Exception {
    String rankingsPath;
    String resultsPath;
    Integer pageRankThreshold;

    if (args.length == 3) {
      rankingsPath = args[0];
      resultsPath = args[1];
      pageRankThreshold = Integer.parseInt(args[2]);
    } else {
      System.err.println("Usage: Q1 <file:///path/to/rankings.csv> <file:///path/to/results/> <pageRankThreshold>");
      return;
    }

    final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

    // Parse input dataset.
    DataSet<Ranking> rankings = env.readCsvFile(rankingsPath).tupleType(Ranking.class);

    // Filter by pageRank, and project out pageURL and pageRank.
    DataSet<Tuple2<String, Integer>> result = rankings.filter(new FilterFunction<Ranking>() {
        public boolean filter(Ranking r) {
          return r.f1 > pageRankThreshold;
        }
      }).project(0, 1).types(String.class, Integer.class);

    // Write output dataset.
    result.writeAsCsv(resultsPath, WriteMode.OVERWRITE);

    // Run.
    JobExecutionResult r = env.execute();

    return;
  }
};
