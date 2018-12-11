package com.shzhangji.flinksandbox;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
  private static final long serialVersionUID = 4651156801756022364L;

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
    for (String word : value.split("\\s+")) {
      out.collect(Tuple2.of(word, 1));
    }
  }
}
