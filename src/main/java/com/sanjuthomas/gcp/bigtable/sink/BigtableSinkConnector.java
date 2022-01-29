package com.sanjuthomas.gcp.bigtable.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
@Slf4j
public class BigtableSinkConnector extends SinkConnector {

  private Map<String, String> config;

  @Override
  public ConfigDef config() {
    return BigtableSinkConfig.CONFIG_DEF;
  }

  @Override
  public void start(final Map<String, String> config) {
    log.info("BigtableSinkConnector is started with config {}", config);
    this.config = config;
  }

  @Override
  public void stop() {
    log.info("BigtableSinkConnector stop is called");
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