package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigtableSinkConfig extends AbstractConfig {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkConfig.class);

  public BigtableSinkConfig(final ConfigDef definition, final Map<?, ?> originals) {
    super(definition, originals);
    logger.info("Original Configs {}", originals);
  }

  public static final String TOPICS = "topics";

  public static final String CONFIG_FILE_LOCATION = "topics.config.files.location";
  private static final String CONFIG_FILE_LOCATION_DOC =
      "the folder in which the bigtable config files are located";
  private static final String CONFIG_FILE_LOCATION_DEFAULT = "config";

  public static ConfigDef CONFIG_DEF = new ConfigDef().define(CONFIG_FILE_LOCATION, Type.STRING,
      CONFIG_FILE_LOCATION_DEFAULT, Importance.HIGH, CONFIG_FILE_LOCATION_DOC);

  public BigtableSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);
    logger.info("Original Configs {}", originals);
  }

}
