/*
 * Copyright (c) 2019 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class BigtableSinkConfig extends AbstractConfig {

  private static final Logger logger = LoggerFactory.getLogger(BigtableSinkConfig.class);
  public static final String TOPICS = "topics";
  public static final String CONFIG_FILE_LOCATION = "config.files.location";
  private static final String CONFIG_FILE_LOCATION_DOC =
      "the folder in which the bigtable config files are located";
  private static final String CONFIG_FILE_LOCATION_DEFAULT = "config";
  public static ConfigDef CONFIG_DEF = new ConfigDef().define(CONFIG_FILE_LOCATION, Type.STRING,
      CONFIG_FILE_LOCATION_DEFAULT, Importance.HIGH, CONFIG_FILE_LOCATION_DOC);

  public BigtableSinkConfig(final ConfigDef definition, final Map<?, ?> originals) {
    super(definition, originals);
    logger.info("Original Configs {}", originals);
  }

  public BigtableSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, true);
    logger.info("Original Configs {}", originals);
  }
}
