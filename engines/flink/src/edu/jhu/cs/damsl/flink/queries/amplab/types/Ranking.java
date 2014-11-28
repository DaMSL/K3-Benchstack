package edu.jhu.cs.damsl.flink.queries.amplab.types;

import org.apache.flink.api.java.tuple.Tuple3;

public class Ranking extends Tuple3<
  String,  // 0. pageURL
  Integer, // 1. pageRank
  Integer  // 2. avgDuration
 > {};
