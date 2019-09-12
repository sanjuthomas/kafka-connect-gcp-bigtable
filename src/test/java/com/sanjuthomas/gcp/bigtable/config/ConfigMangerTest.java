package com.sanjuthomas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.ConfigManger;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;.
import com.sanjuthomas.gcp.bigtable.Integration;

import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Integration
public class ConfigMangerTest {

  @BeforeAll
  public static void setup() throws JsonParseException, JsonMappingException, IOException {
    ConfigManger.load("src/main/resources/configs/", "demo-topic");
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldGetTransformerForGivenTopic(final SinkRecord record) {
    final Transformer<SinkRecord, WritableRow> transformer = ConfigManger.transformer("demo-topic");
    final WritableRow row = transformer.transform(record);
    assertEquals("NYQ_MMM", row.rowKey());
    final List<WritableFamilyCells> cells = row.familyCells();
    assertEquals(2, cells.size());
    assertEquals("data", cells.get(0).family());
    assertEquals("metadata", cells.get(1).family());
  }

  @Test
  public void shouldGetWriterForGivenTopic() {
    final Writer<WritableRow, Boolean> writer = ConfigManger.writer("demo-topic");
    assertNotNull(writer);
  }

}
