package com.sanjuthomas.gcp.bigtable.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;

/**
 * 
 * Class responsible for creating and caching writer objects. There will be one instance of this class per Task thread.
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Evolving
@Slf4j
public class WriterProvider {

  private final Map<String, Writer<WritableRow, Boolean>> writerMap = new HashMap<>();

  private ConfigProvider configProvider;

  public WriterProvider(final ConfigProvider configProvider) {
    log.info("WriterProvider is created by task id {}", Thread.currentThread().getId());
    this.configProvider = configProvider;
  }

  /**
   * Return the writer for the given topic.
   * 
   * @param topic
   * @return
   */
  public Writer<WritableRow, Boolean> writer(final String topic) {
    try {
      if (!writerMap.containsKey(topic)) {
        return createAndCacheWriter(topic);
      }
      return writerMap.get(topic);
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  private Writer<WritableRow, Boolean> createAndCacheWriter(final String topic) throws IOException {
    final WriterConfig writerConfig = configProvider.config(topic).getWriterConfig();
    final BigtableWriter bigtableWriter = new BigtableWriter(writerConfig, new ClientProvider(writerConfig).client());
    writerMap.put(topic, bigtableWriter);
    log.info("Writer created for topic {} and cached for task id {}", topic, Thread.currentThread().getId());
    return bigtableWriter;
  }

  /**
   * Remove the writer upon write error so that the next client get a new one.
   * 
   * @param topic
   */
  public void remove(final String topic) {
    if (writerMap.containsKey(topic)) {
      writerMap.remove(topic).close();
    }
  }
}