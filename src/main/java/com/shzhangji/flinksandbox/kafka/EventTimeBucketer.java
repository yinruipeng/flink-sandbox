package com.shzhangji.flinksandbox.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

public class EventTimeBucketer implements Bucketer<String> {
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public Path getBucketPath(Clock clock, Path basePath, String element) {
    String partitionValue;
    try {
      partitionValue = getPartitionValue(element);
    } catch (Exception e) {
      partitionValue = "00000000";
    }
    return new Path(basePath, "dt=" + partitionValue);
  }

  private String getPartitionValue(String element) throws Exception {
    JsonNode node = mapper.readTree(element);
    long date = (long) (node.path("timestamp").floatValue() * 1000);
    return new SimpleDateFormat("yyyyMMdd").format(new Date(date));
  }
}
