package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.ConfigProvider;
import com.sanjuthomas.gcp.bigtable.config.WriterProvider;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigtableSinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkTask.class);
  private static final Set<String> assingedTopics = new LinkedHashSet<>();
  private ConfigProvider configProvider;
  @VisibleForTesting
  WriterProvider writerProvider;

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    logger.info("Data arrived in the Bigtable Sink Task, sinkRecords count is {}",
        sinkRecords.size());
    for (final SinkRecord sr : sinkRecords) {
      final WritableRow row = configProvider.transformer(sr.topic()).transform(sr);
      logger.info("transformed row {}", row);
      writerProvider.writer(sr.topic()).buffer(row);
    }
    assingedTopics.forEach(at -> {
      final Writer<WritableRow, Boolean> writer = writerProvider.writer(at);
      if (writer.bufferSize() > 0) {
        writer.flush();
      }
    });
  }

  @Override
  public void start(final Map<String, String> config) {
    logger.info("{} started with config {}", this, config);
    this.configProvider = new ConfigProvider();
    final String topics = config.get(BigtableSinkConfig.TOPICS);
    final String configFileLocation = config.get(BigtableSinkConfig.CONFIG_FILE_LOCATION);
    Preconditions.checkNotNull(topics,
        "topics is a mandatory config in the bigtable-sink.properties");
    Preconditions.checkNotNull(configFileLocation,
        "topics.config.files.location is a mandatory config in the bigtable-sink.properties");
    for (final String topic : topics.split(",")) {
      configProvider.load(configFileLocation, StringUtils.trim(topic));
    }
    this.writerProvider = new WriterProvider(configProvider);
  }

  @Override
  public void open(Collection<TopicPartition> topicPartitions) {
    topicPartitions.forEach(tp -> assingedTopics.add(tp.topic()));
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    logger.info("flush is called for {}", currentOffsets.keySet());
  }

  @Override
  public void stop() {
    assingedTopics.forEach(at -> writerProvider.writer(at).close());
    logger.info("{} stopped", this);
  }

}
