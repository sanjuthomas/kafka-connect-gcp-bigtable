package com.sanjuthomas.gcp.bigtable.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.common.base.MoreObjects;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class WriterProvider {

  private ConfigProvider configProvider;

  private static final Map<String, Writer<WritableRow, Boolean>> writerMap =
      new ConcurrentHashMap<>();

  public WriterProvider(final ConfigProvider configProvider) {
    this.configProvider = configProvider;
  }

  public Writer<WritableRow, Boolean> writer(final String topic) {
    try {
      return MoreObjects.firstNonNull(writerMap.get(topic), createAndCacheWriter(topic));
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  private Writer<WritableRow, Boolean> createAndCacheWriter(final String topic)
      throws FileNotFoundException, IOException {
    WriterConfig writerConfig = configProvider.config(topic).writerConfig();
    final BigtableWriter bigtableWriter =
        new BigtableWriter(writerConfig, new ClientProvider(writerConfig).client());
    writerMap.put(topic, bigtableWriter);
    return bigtableWriter;
  }

}
