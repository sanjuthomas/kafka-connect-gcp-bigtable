package com.sanjuthomas.gcp.bigtable.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class BigtableSinkConnectorTest {

  private BigtableSinkConnector bigtableSinkConnector;

  @BeforeEach
  public void setup() {
    bigtableSinkConnector = new BigtableSinkConnector();
    bigtableSinkConnector.start(Collections.emptyMap());
  }

  @Test
  public void shouldGetTaskConfigs() {
    final List<Map<String, String>> taskConfigs = bigtableSinkConnector.taskConfigs(1);
    assertEquals(1, taskConfigs.size());
    assertEquals(0, taskConfigs.get(0).size());
  }
  
  @Test
  public void shouldGetTaskClass() {
    assertEquals(BigtableSinkTask.class, bigtableSinkConnector.taskClass());
  }
  
  @Test
  public void shouldGetVerson() {
    assertEquals("1.0", bigtableSinkConnector.version());
  }
  
  @Test
  public void shouldGetConfigDef() {
    assertEquals(BigtableSinkConfig.CONFIG_DEF, bigtableSinkConnector.config());
  }
}
