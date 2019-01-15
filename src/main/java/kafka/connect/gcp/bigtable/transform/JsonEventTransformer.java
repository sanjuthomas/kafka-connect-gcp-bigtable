package kafka.connect.gcp.bigtable.transform;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import kafka.connect.gcp.bigtable.Parser;
import kafka.connect.gcp.bigtable.Transformer;
import kafka.connect.gcp.bigtable.bean.WritableCell;
import kafka.connect.gcp.bigtable.bean.WritableCells;
import kafka.connect.gcp.bigtable.bean.WritableRow;
import kafka.connect.gcp.bigtable.config.TransformerConfig;
import kafka.connect.gcp.bigtable.exception.RowKeyNotFoundException;
import kafka.connect.gcp.bigtable.transform.key.DefaultKeyParser;

/**
 * This default transformer assumes that the values in the sink records are Maps. If your data is in
 * different format, please write another transformer implementation and change the configuration.
 *
 * @author Sanju Thomas
 *
 */
public class JsonEventTransformer implements Transformer<SinkRecord, WritableRow> {

  private final Parser<Object, String> keyParser;
  private final TransformerConfig config;
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonEventTransformer(final TransformerConfig config) {
    this.config = config;
    this.keyParser = new DefaultKeyParser(config.keyQualifies(), config.keyDelimiter());
  }

  @Override
  public WritableRow transform(final SinkRecord record) {
    final Map<String, Object> payload =
        OBJECT_MAPPER.convertValue(record.value(), new TypeReference<Map<String, Object>>() {});
    this.addMetadata(record, payload);
    final WritableRow row = new WritableRow(this.rowKey(record, payload));
    for (final String family : this.config.families()) {
      row.addCell(this.createCells(family, payload));
    }
    return row;
  }

  void addMetadata(final SinkRecord record, final Map<String, Object> payload) {
    payload.put("create_time", record.timestamp());
    payload.put("processing_time", System.currentTimeMillis());
    payload.put("topic", record.topic());
  }

  @VisibleForTesting
  WritableCells createCells(final String family, final Map<String, ? extends Object> row) {
    final Map<String, Object> filteredRow = this.filterRow(family, row);
    final List<WritableCell> cells = filteredRow.entrySet().stream()
        .map(e -> new WritableCell(TypeUtils.toByteString(e.getKey()),
            TypeUtils.toByteString(e.getValue())))
        .collect(Collectors.toList());
    return new WritableCells(family, cells);
  }

  @VisibleForTesting
  Map<String, Object> filterRow(final String family, final Map<String, ? extends Object> row) {
    return row.entrySet().stream()
        .filter((e) -> this.config.familyQualifiers(family).contains(e.getKey()))
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  @VisibleForTesting
  String rowKey(final SinkRecord record, final Map<String, ? extends Object> row) {
    String rowKey = null;
    if (this.config.keyQualifies().isEmpty()) {
      rowKey = this.keyParser.parse(record);
    } else {
      rowKey = this.keyParser.parse(row);
    }
    if (rowKey == null) {
      throw new RowKeyNotFoundException(
          "keyQualifiers are not configured and there is not key found in the SinkRecord!");
    }
    return rowKey;
  }
}
