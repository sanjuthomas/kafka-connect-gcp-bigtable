package com.sanjuthomas.gcp.bigtable.config;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;
import com.sanjuthomas.gcp.bigtable.exception.TransformInitializationException;

/**
 * 
 * Class responsible for creating and caching the configuration(s) for task(s). Every task instance
 * would create an instance of the ConfigProvider during the start up and provide the
 * configuration(s) during the life of the task. Per design, there would be one configuration file
 * per topic and the configuration file is written in a .yml file. Please refer {@link Config} to
 * understand an example configuration.
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class ConfigProvider {

  private static final Logger logger = LoggerFactory.getLogger(ConfigProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private final Map<String, Config> configs = new ConcurrentHashMap<>();
  private final Map<String, Transformer<SinkRecord, WritableRow>> transformerMap = new HashMap<>();

  public ConfigProvider() {
    logger.info("ConfigProvider is created by thread id {}.", Thread.currentThread().getId());
  }

  /**
   * Load configuration for a given topic from the given configFileLocation. There should be one
   * configuration file per topic in the configFileLocation.
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
      if (!transformerMap.containsKey(topic)) {
        synchronized (transformerMap) {
          return transformerMap.get(topic) != null ? transformerMap.get(topic)
              : createAndCacheTransformer(topic);
        }
      }
      return transformerMap.get(topic);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException
        | InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new TransformInitializationException(e.getMessage());
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
    @SuppressWarnings("unchecked")
    final Transformer<SinkRecord, WritableRow> jsonEventTransformer =
        (Transformer<SinkRecord, WritableRow>) constructor.newInstance(transformerConfig);
    transformerMap.put(topic, jsonEventTransformer);
    logger.info("Transformer is created by thread id {}.", Thread.currentThread().getId());
    return jsonEventTransformer;
  }
}
