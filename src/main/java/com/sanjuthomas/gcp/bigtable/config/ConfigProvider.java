package com.sanjuthomas.gcp.bigtable.config;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.sink.SinkRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.MoreObjects;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ConfigProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private static final Map<String, Config> configs = new ConcurrentHashMap<>();
  private static final Map<String, Transformer<SinkRecord, WritableRow>> transformerMap =
      new ConcurrentHashMap<>();

  /**
   * Load configuration for a given topic from the given configFileLocation.
   * There should be one configuration file per topic in the configFileLocation.
   * 
   * @param configFileLocation
   * @param topic
   */
  public void load(final String configFileLocation, final String topic) {
    try {
      final String configFile = String.format("%s/%s.%s", configFileLocation, topic, "yml");
      final Config config = MAPPER.readValue(new File(configFile), Config.class);
      configs.put(topic, config);
    } catch (final Exception e) {
      throw new BigtableSinkInitializationException(e.getMessage(), e);
    }
  }

  /**
   * Return the Configuration for given topic.
   * 
   * @param topic
   * @return
   */
  public Config config(final String topic) {
    return configs.get(topic);
  }

  /**
   * Return the Transformer for given a topic.
   * 
   * @param topic
   * @return Transformer
   */
  public Transformer<SinkRecord, WritableRow> transformer(final String topic) {
    try {
      return MoreObjects.firstNonNull(transformerMap.get(topic), createAndCacheTransformer(topic));
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException
        | InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  private Transformer<SinkRecord, WritableRow> createAndCacheTransformer(final String topic)
      throws NoSuchMethodException, SecurityException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException {
    final Config config = configs.get(topic);
    final TransformerConfig transformerConfig = new TransformerConfig(config.getKeyQualifiers(),
        config.getKeyDelimiter(), config.getFamilies(), config.familyQualifiersMappings());
    final Constructor<?> constructor =
        Class.forName(config.getTransformer()).getConstructor(TransformerConfig.class);
    final Transformer<SinkRecord, WritableRow> jsonEventTransformer =
        (Transformer<SinkRecord, WritableRow>) constructor.newInstance(transformerConfig);
    transformerMap.put(topic, jsonEventTransformer);
    return jsonEventTransformer;
  }
}
