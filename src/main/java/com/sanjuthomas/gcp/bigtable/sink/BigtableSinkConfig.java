package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
@Slf4j
public class BigtableSinkConfig extends AbstractConfig {

  private static final String CONFIG_FILE_LOCATION_DOC = "the folder in which the bigtable config files are located";
  private static final String CONFIG_FILE_LOCATION_DEFAULT = "config";
  public static final String TOPICS = "topics";
  public static final String CONFIG_FILE_LOCATION = "config.files.location";
  public static ConfigDef CONFIG_DEF = new ConfigDef().define(CONFIG_FILE_LOCATION, Type.STRING, CONFIG_FILE_LOCATION_DEFAULT, Importance.HIGH, CONFIG_FILE_LOCATION_DOC);
  
  public BigtableSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);
    log.info("Original Configs {}", originals);
  }
}