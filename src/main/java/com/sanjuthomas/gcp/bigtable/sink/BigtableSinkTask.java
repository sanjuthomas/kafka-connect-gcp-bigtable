package com.sanjuthomas.gcp.bigtable.sink;

import com.sanjuthomas.gcp.bigtable.config.ConfigManger;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Preconditions;

import com.sanjuthomas.gcp.bigtable.bean.WritableRow;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigtableSinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkTask.class);
  private static final Set<String> assingedTopics = new LinkedHashSet<>();

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    logger.info("Data arrived in the Bigtable Sink Task, the count is {}", sinkRecords.size());
    for (final SinkRecord sr : sinkRecords) {
      final WritableRow row = ConfigManger.transformer(sr.topic()).transform(sr);
      logger.info("transformed row {}", row);
      ConfigManger.writer(sr.topic()).buffer(row);
    }
    assingedTopics.forEach(at -> ConfigManger.writer(at).flush());
  }

  @Override
  public void start(final Map<String, String> config) {
    logger.info("{} started with config {}", this, config);
    final String topics = config.get(BigtableSinkConfig.TOPICS);
    final String configFileLocation = config.get(BigtableSinkConfig.CONFIG_FILE_LOCATION);
    Preconditions.checkNotNull(topics,
        "topics is a mandatory config in the bigtable-sink.properties");
    Preconditions.checkNotNull(configFileLocation,
        "topics.config.files.location is a mandatory config in the bigtable-sink.properties");
    for(String topic : topics.split(",")) {
    	ConfigManger.load(configFileLocation, topic);
    }
  }

  @Override
  public void open(Collection<TopicPartition> topicPartitions) {
	  topicPartitions.forEach(tp -> assingedTopics.add(tp.topic()));
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    logger.info("flush is called for {}", currentOffsets.keySet());
    currentOffsets.entrySet().stream().map(e -> ConfigManger.writer(e.getKey().topic()).flush());
  }

  @Override
  public void stop() {
    logger.info("{} stopped", this);
  }

}
