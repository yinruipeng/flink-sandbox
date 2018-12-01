package com.shzhangji.flinksandbox;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchJob {
  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> ds = env.readTextFile("data/wordcount.txt");
    ds.flatMap(new Tokenizer()).groupBy(0).sum(1).print();

    env.execute("Flink Batch Java API Skeleton");
  }

  public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 4651156801756022364L;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word : value.split("\\s+")) {
        out.collect(new Tuple2<>(word, 1));
      }
    }
  }
}
