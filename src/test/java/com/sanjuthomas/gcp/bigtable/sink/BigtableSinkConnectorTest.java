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
  public void setUp() {
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
