package com.sanjuthomas.gcp.bigtable.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.sanjuthomas.gcp.bigtable.config.TransformerConfig;
import com.sanjuthomas.gcp.bigtable.exception.RowKeyNotFoundException;
import com.sanjuthomas.gcp.resolvers.KeylessSinkRecordResolver;
import com.sanjuthomas.gcp.resolvers.MapEventResolver;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class JsonEventTransformerTest {

  private JsonEventTransformer transformer;
  private JsonEventTransformer transformerWithoutKeyQualifier;
  private final List<String> keyQualifiers = Arrays.asList(new String[] {"symbol"});
  private final List<String> families = Arrays.asList(new String[] {"data", "metadata"});
  private Map<String, List<String>> familyToQualifierMapping;

  @BeforeEach
  void setup() {
    this.familyToQualifierMapping = new HashMap<>();
    this.familyToQualifierMapping.put("data",
        Arrays.asList(new String[] {"symbol", "name", "sector"}));
    this.familyToQualifierMapping.put("metadata",
        Arrays.asList(new String[] {"create_time", "processing_time", "topic"}));
    final TransformerConfig config = new TransformerConfig(this.keyQualifiers, "_", this.families,
        this.familyToQualifierMapping);
    this.transformer = new JsonEventTransformer(config);
    final TransformerConfig noKeyQualifiersConfig = new TransformerConfig(Collections.emptyList(),
        "_", this.families, this.familyToQualifierMapping);
    this.transformerWithoutKeyQualifier = new JsonEventTransformer(noKeyQualifiersConfig);
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldTransformToWritableRowsWhenKeyQualifiersIsGiven(final SinkRecord record) {
    final WritableRow rows = this.transformer.transform(record);
    assertEquals("MMM", rows.rowKey());
    final List<WritableFamilyCells> cells = rows.familyCells();
    final WritableFamilyCells data = cells.get(0);
    assertEquals("data", data.family());
    final WritableFamilyCells metadata = cells.get(1);
    assertEquals("metadata", metadata.family());
    assertEquals(3, data.cells().size());
    assertEquals("symbol", data.cells().get(0).qualifier().toStringUtf8());
    assertEquals("MMM", data.cells().get(0).value().toStringUtf8());
    assertEquals("name", data.cells().get(1).qualifier().toStringUtf8());
    assertEquals("3MCompany", data.cells().get(1).value().toStringUtf8());
    assertEquals("sector", data.cells().get(2).qualifier().toStringUtf8());
    assertEquals("Industrials", data.cells().get(2).value().toStringUtf8());
    assertEquals(3, metadata.cells().size());
  }

  @Test
  @ExtendWith(MapEventResolver.class)
  public void shouldFilterOutUnknownElementsInTheMessage(final Map<String, String> events) {
    final Map<String, Object> filteredRow = this.transformer.filterRow("data", events);
    assertNull(filteredRow.get("exchange"));
    assertEquals("MMM", filteredRow.get("symbol"));
    assertEquals("3MCompany", filteredRow.get("name"));
    assertEquals("Industrials", filteredRow.get("sector"));
  }

  @Test
  @ExtendWith({SinkRecordResolver.class, MapEventResolver.class})
  public void shouldGetRowKeyWhenKeyQualifierIsEmptyAndKeyIsGivenInSinkRecord(
      final SinkRecord record, final Map<String, String> mapEvent) {
    assertEquals("MMM", this.transformerWithoutKeyQualifier.rowKey(record, mapEvent));
  }

  @Test
  @ExtendWith({KeylessSinkRecordResolver.class, MapEventResolver.class})
  public void shouldGetRowKeyWhenKeyQualifierIsNotEmptyAndKeyIsNotGivenInSinkRecord(
      final SinkRecord record, final Map<String, String> mapEvent) {
    assertEquals("MMM", this.transformer.rowKey(record, mapEvent));
  }

  @Test
  @ExtendWith({KeylessSinkRecordResolver.class, MapEventResolver.class})
  public void shouldNotGetRowKeyWhenKeyQualifierIsEmptyAndKeyIsNotGivenInSinkRecord(
      final SinkRecord record, final Map<String, String> mapEvent) {
    final RowKeyNotFoundException exception = assertThrows(RowKeyNotFoundException.class, () -> {
      this.transformerWithoutKeyQualifier.rowKey(record, mapEvent);
    });
    assertEquals("keyQualifiers are not configured and there is not key found in the SinkRecord!",
        exception.getMessage());
  }

  @Test
  @ExtendWith({SinkRecordResolver.class, MapEventResolver.class})
  public void shouldCreateRow(final SinkRecord record, final Map<String, String> mapEvent) {
    final WritableFamilyCells cells = this.transformer.createCells("data", mapEvent);
    assertEquals("data", cells.family());
  }

}
