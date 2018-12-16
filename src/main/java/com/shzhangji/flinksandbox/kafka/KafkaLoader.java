package com.shzhangji.flinksandbox.kafka;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class KafkaLoader {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(5000);
    env.setParallelism(2);

    // source
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(
        "zj_test", new SimpleStringSchema(), props);
    DataStream<String> stream = env.addSource(consumer);

    // sink
    BucketingSink<String> sink = new BucketingSink<>("/tmp/kafka-loader");
    sink.setBucketer(new EventTimeBucketer());
    sink.setBatchRolloverInterval(5000);
    stream.addSink(sink);

    env.execute();
  }
}
