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

package com.sanjuthomas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.TransformInitializationException;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ConfigProviderTest {

  private ConfigProvider configProvider;
  private ConfigProvider fakeConfigProvider;

  @BeforeEach
  public void setUp() throws JsonParseException, JsonMappingException, IOException {
    configProvider = new ConfigProvider();
    configProvider.load("src/test/resources/", "demo-topic");
    fakeConfigProvider = new ConfigProvider();
    fakeConfigProvider.load("src/test/resources/", "fake-topic");
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldGetTransformerForGivenTopic(final SinkRecord record) {
    final Transformer<SinkRecord, WritableRow> transformer =
        configProvider.transformer("demo-topic");
    final WritableRow row = transformer.transform(record);
    assertEquals("NYQ_MMM", row.rowKey());
    final List<WritableFamilyCells> cells = row.familyCells();
    assertEquals(2, cells.size());
    assertEquals("data", cells.get(0).family());
    assertEquals("metadata", cells.get(1).family());
    final Transformer<SinkRecord, WritableRow> transformer1 =
        configProvider.transformer("demo-topic");
    assertEquals(transformer1.hashCode(), transformer.hashCode());
    assertEquals(configProvider.config("demo-topic").hashCode(),
        configProvider.config("demo-topic").hashCode());
  }

  @Test
  public void shouldNotInitializeTrasnformer() {
    Assertions.assertThrows(TransformInitializationException.class, () -> {
      fakeConfigProvider.transformer("fake-topic");
    });
    assertEquals(1, 1);
  }

  @Test
  public void shouldGetWriterConfig() {
    assertEquals("demo-table", configProvider.config("demo-topic").getWriterConfig().table());
    assertEquals("/Users/sanjuthomas/keys/civic-athlete-251623-e16dce095204.json",
        configProvider.config("demo-topic").getWriterConfig().keyFile());
    assertEquals("demo-project", configProvider.config("demo-topic").getWriterConfig().project());
    assertEquals("demo-instance", configProvider.config("demo-topic").getWriterConfig().instance());
  }

  @Test
  public void shouldGetErrorHandlerConfig() {
    assertEquals(3, configProvider.config("demo-topic").getWriterConfig().getErrorHandlerConfig()
        .maxRetryCount());
    assertEquals(3, configProvider.config("demo-topic").getWriterConfig().getErrorHandlerConfig()
        .retryBackoffSeconds());
    assertEquals(true, configProvider.config("demo-topic").getWriterConfig().getErrorHandlerConfig()
        .exponentialBackoff());
  }

}
