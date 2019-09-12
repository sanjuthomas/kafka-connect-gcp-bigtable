package com.sanjuthomas.gcp.bigtable.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.sink.SinkRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.MoreObjects;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;
import com.sanjuthomas.gcp.bigtable.transform.JsonEventTransformer;
import com.sanjuthomas.gcp.bigtable.writer.BigtableWriter;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.Writer;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ConfigManger {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private static final Map<String, Config> configs = new ConcurrentHashMap<>();
  private static final Map<String, Transformer<SinkRecord, WritableRow>> transformerMap =
      new ConcurrentHashMap<>();
  private static final Map<String, Writer<WritableRow, Boolean>> writerMap =
      new ConcurrentHashMap<>();

  public static void load(final String configFileLocation, final String topic) {
    try {
      final String configFile = String.format("%s/%s.%s", configFileLocation, topic, "yml");
      final Config config = MAPPER.readValue(new File(configFile), Config.class);
      configs.put(topic, config);
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  public static Transformer<SinkRecord, WritableRow> transformer(final String topic) {
    return MoreObjects.firstNonNull(transformerMap.get(topic), createAndCacheTransformer(topic));
  }

  public static Writer<WritableRow, Boolean> writer(final String topic) {
    try {
      return MoreObjects.firstNonNull(writerMap.get(topic), createAndCacheWriter(topic));
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  private static Writer<WritableRow, Boolean> createAndCacheWriter(final String topic)
      throws FileNotFoundException, IOException {
    final Config config = configs.get(topic);
    final WriterConfig writerConfig = new WriterConfig(config.getKeyFile(), config.getProject(),
        config.getInstance(), config.getTable());
    final BigtableWriter bigtableWriter = new BigtableWriter(writerConfig);
    writerMap.put(topic, bigtableWriter);
    return bigtableWriter;
  }

  private static Transformer<SinkRecord, WritableRow> createAndCacheTransformer(
      final String topic) {
    final Config config = configs.get(topic);
    final TransformerConfig transformerConfig = new TransformerConfig(config.getKeyQualifiers(),
        config.getKeyDelimiter(), config.getFamilies(), config.familyQualifiersMappings());
    final JsonEventTransformer jsonEventTransformer = new JsonEventTransformer(transformerConfig);
    transformerMap.put(topic, jsonEventTransformer);
    return jsonEventTransformer;
  }
}
