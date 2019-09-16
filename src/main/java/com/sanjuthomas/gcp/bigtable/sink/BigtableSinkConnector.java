package com.sanjuthomas.gcp.bigtable.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 *
 */
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
    logger.info("stop called");
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
