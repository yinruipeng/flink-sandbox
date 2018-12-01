package com.shzhangji.flinksandbox;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class BatchJob {
  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> ds = env.readTextFile("data/wordcount.txt");
    ds.flatMap(new Tokenizer()).groupBy(0).sum(1).print();

    env.execute("Flink Batch Java API Skeleton");
  }
}
