package kafka.connect.gcp.bigtable.sink;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.client.util.Preconditions;
import kafka.connect.gcp.bigtable.Writer;
import kafka.connect.gcp.bigtable.bean.WritableRow;
import kafka.connect.gcp.bigtable.config.ConfigManger;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigtableSinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkTask.class);

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    logger.info("Data arrived in the Bigtable Sink Task, the count is {}", sinkRecords.size());
    for (final SinkRecord sr : sinkRecords) {
      final Writer<WritableRow, Boolean> writer = ConfigManger.writer(sr.topic());
      final WritableRow row = ConfigManger.transformer(sr.topic()).transform(sr);
      logger.info("transformed row {}", row);
      writer.buffer(row);
    }
  }

  @Override
  public void start(final Map<String, String> config) {
    logger.info("{} started with config {}", this, config);
    final String topic = config.get(BigtableSinkConfig.TOPICS);
    final String configFileLocation = config.get(BigtableSinkConfig.CONFIG_FILE_LOCATION);
    Preconditions.checkNotNull(topic,
        "topics is a mandatory config in the bigtable-sink.properties");
    Preconditions.checkNotNull(configFileLocation,
        "topics.config.files.location is a mandatory config in the bigtable-sink.properties");
    ConfigManger.load(configFileLocation, topic);
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
