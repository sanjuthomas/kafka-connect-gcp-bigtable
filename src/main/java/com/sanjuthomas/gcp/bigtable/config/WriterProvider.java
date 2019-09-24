package com.sanjuthomas.gcp.bigtable.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;

/**
 * 
 * Class responsible for creating and caching writer objects.
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Evolving
public class WriterProvider {

  private static final Logger logger = LoggerFactory.getLogger(WriterProvider.class);
  private final Map<String, Writer<WritableRow, Boolean>> writerMap = new HashMap<>();

  private ConfigProvider configProvider;

  public WriterProvider(final ConfigProvider configProvider) {
    logger.info("WriterProvider is created by thread id {}.", Thread.currentThread().getId());
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
        synchronized (writerMap) {
          return writerMap.get(topic) != null ? writerMap.get(topic) : createAndCacheWriter(topic);
        }
      }
      return writerMap.get(topic);
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  private Writer<WritableRow, Boolean> createAndCacheWriter(final String topic)
      throws FileNotFoundException, IOException {
    final WriterConfig writerConfig = configProvider.config(topic).getWriterConfig();
    final BigtableWriter bigtableWriter =
        new BigtableWriter(writerConfig, new ClientProvider(writerConfig).client());
    writerMap.put(topic, bigtableWriter);
    logger.info("Writer created for topic {} and cached.", topic);
    return bigtableWriter;
  }

  /**
   * Remove the writer upon write error so that the next client get a new one.
   * 
   * @param topic
   */
  public void remove(final String topic) {
    if (writerMap.containsKey(topic)) {
      synchronized (writerMap) {
        writerMap.remove(topic).close();
      }
    }
  }

}
