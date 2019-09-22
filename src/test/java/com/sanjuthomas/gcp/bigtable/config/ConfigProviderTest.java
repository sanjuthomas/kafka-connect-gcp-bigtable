package com.sanjuthomas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ConfigProviderTest {

  private ConfigProvider configProvider;

  @BeforeEach
  public void setUp() throws JsonParseException, JsonMappingException, IOException {
    configProvider = new ConfigProvider();
    configProvider.load("src/test/resources/", "demo-topic");
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
