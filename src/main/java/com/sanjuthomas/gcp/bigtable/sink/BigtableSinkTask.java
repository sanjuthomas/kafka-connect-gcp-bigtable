package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.ConfigProvider;
import com.sanjuthomas.gcp.bigtable.config.WriterProvider;
import com.sanjuthomas.gcp.bigtable.exception.BigtableWriteFailedException;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;

/**
 * Refer to super class documentation for general information about the {@link SinkTask}. This
 * class is responsible for taking a batch of SinkRecord(s), call the given {@link Transformer}
 * to transform SinkRecord(s) to Bigtable writable rows, and call the {@link BigtableWriter} to
 * buffer and flush the rows to Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 * 
 */
@Evolving
public class BigtableSinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkTask.class);
  private static final Set<String> assingedTopics = new LinkedHashSet<>();
  private ConfigProvider configProvider;
  @VisibleForTesting
  WriterProvider writerProvider;
  @VisibleForTesting
  boolean continueAfterWriteError;

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    logger.debug("data arrived in the Bigtable sink task, record count was {}", sinkRecords.size());
    for (final SinkRecord sr : sinkRecords) {
      final WritableRow row = configProvider.transformer(sr.topic()).transform(sr);
      logger.debug("transformed row {}", row);
      writerProvider.writer(sr.topic()).buffer(row);
    }
    assingedTopics.forEach(topic -> {
      final Writer<WritableRow, Boolean> writer = writerProvider.writer(topic);
      if (writer.bufferSize() > 0) {
        try {
          writer.flush();
        } catch (BigtableWriteFailedException e) {
          writerProvider.remove(topic);
          if (continueAfterWriteError) {
            logger.error(
                "swallow the error and continue to next batch, all or part of the batch is lost and the batch size was {}",
                sinkRecords.size());
          } else {
            throw e;
          }
        }
      }
    });
  }

  @Override
  public void start(final Map<String, String> config) {
    logger.info("task {} started with config {}", Thread.currentThread().getId(), config);
    this.configProvider = new ConfigProvider();
    final String topics = config.get(BigtableSinkConfig.TOPICS);
    continueAfterWriteError = Boolean.valueOf(
        Objects.toString(config.get(BigtableSinkConfig.CONTINUE_AFTER_WRITE_ERROR), "false"));
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
    logger.debug("flush is called for {} in task {}", currentOffsets.keySet(),
        Thread.currentThread().getId());
  }

  @Override
  public void stop() {
    assingedTopics.forEach(at -> writerProvider.writer(at).close());
    logger.info("task {} stopped", Thread.currentThread().getId());
  }

}
