package com.shzhangji.flinksandbox;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> ds = env.readTextFile("data/wordcount.txt");
    ds.flatMap(new Tokenizer()).keyBy(0).sum(1).print();

    env.execute("Flink Streaming Java API Skeleton");
  }
}
