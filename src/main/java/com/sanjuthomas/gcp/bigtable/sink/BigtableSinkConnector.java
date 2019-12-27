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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class BigtableSinkConnector extends SinkConnector {

  private static final Logger logger = LoggerFactory.getLogger(SinkConnector.class);
  private Map<String, String> config;

  @Override
  public ConfigDef config() {
    return BigtableSinkConfig.CONFIG_DEF;
  }

  @Override
  public void start(final Map<String, String> config) {
    logger.info("BigtableSinkConnector is started with config {}", config);
    this.config = config;
  }

  @Override
  public void stop() {
    logger.info("BigtableSinkConnector stop is called");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return BigtableSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int taskCunt) {
    final List<Map<String, String>> configs = new ArrayList<>(taskCunt);
    for (int i = 0; i < taskCunt; ++i) {
      configs.add(this.config);
    }
    return configs;
  }

  @Override
  public String version() {
    return "1.0";
  }

}
