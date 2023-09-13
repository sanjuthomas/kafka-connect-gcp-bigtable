/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.ConfigProvider;
import com.sanjuthomas.gcp.bigtable.config.WriterProvider;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;

/**
 * Refer to super class documentation for general information about the
 * {@link SinkTask}. This class
 * is responsible for taking a batch of SinkRecord(s), call the given
 * {@link Transformer} to
 * transform SinkRecord(s) to Bigtable writable rows, and call the
 * {@link BigtableWriter} to buffer
 * and flush the rows to Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 */
@Evolving
@Slf4j
public class BigtableSinkTask extends SinkTask {

  private static final Set<String> assignedTopics = new LinkedHashSet<>();
  private ConfigProvider configProvider;
  @VisibleForTesting
  WriterProvider writerProvider;

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    log.debug("data arrived in the Bigtable sink task, record count was {}", sinkRecords.size());
    for (final SinkRecord sr : sinkRecords) {
      final WritableRow row = configProvider.transformer(sr.topic()).transform(sr);
      log.debug("transformed row {}", row);
      writerProvider.writer(sr.topic()).buffer(row);
    }
    assignedTopics.forEach(topic -> {
      final Writer<WritableRow, Boolean> writer = writerProvider.writer(topic);
      if (writer.bufferSize() > 0) {
        try {
          writer.flush();
        } catch (Exception e) {
          writerProvider.remove(topic);
          log.error(e.getMessage(), e);
          throw e;
        }
      }
    });
  }

  /**
   * Every task would have it's on ConfigProvider and WriterProvider - so
   * nothing shared among the tasks.
   */
  @Override
  public void start(final Map<String, String> config) {
    log.info("task {} started with config {}", Thread.currentThread().getId(), config);
    this.configProvider = new ConfigProvider();
    final String topics = config.get(BigtableSinkConfig.TOPICS);
    final String configFileLocation = config.get(BigtableSinkConfig.CONFIG_FILE_LOCATION);
    Preconditions.checkNotNull(topics,
        "topics is a mandatory config in the bigtable-sink.properties");
    Preconditions.checkNotNull(configFileLocation,
        "topics.config.files.location is a mandatory config in the bigtable-sink.properties");
    for (final String topic : topics.split(",")) {
      log.info("task {} loading configuration for topic {}", Thread.currentThread().getId(), topic);
      configProvider.load(configFileLocation, topic.trim());
    }
    this.writerProvider = new WriterProvider(configProvider);
  }

  @Override
  public void open(Collection<TopicPartition> topicPartitions) {
    topicPartitions.forEach(tp -> assignedTopics.add(tp.topic()));
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    log.debug("flush is called for {} in task {}", currentOffsets.keySet(),
        Thread.currentThread().getId());
  }

  @Override
  public void stop() {
    assignedTopics.forEach(at -> writerProvider.writer(at).close());
    log.info("task {} stopped", Thread.currentThread().getId());
  }
}