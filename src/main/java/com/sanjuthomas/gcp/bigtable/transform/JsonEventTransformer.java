/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.transform;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.connect.sink.SinkRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Parser;
import com.sanjuthomas.gcp.bigtable.Transformer;
import com.sanjuthomas.gcp.bigtable.bean.WritableCell;
import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.TransformerConfig;
import com.sanjuthomas.gcp.bigtable.exception.RowKeyNotFoundException;
import com.sanjuthomas.gcp.bigtable.transform.key.DefaultKeyParser;

/**
 * This default transformer assumes that the values in the sink records are Maps. If your data is in
 * different format, please write another transformer implementation and change the configuration.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 */
@Stable
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
    final Map<String, Object> payload = OBJECT_MAPPER.convertValue(record.value(),
      new TypeReference<>() {
      });
    this.addMetadata(record, payload);
    final WritableRow row = new WritableRow(this.rowKey(record, payload));
    for (final String family : this.config.families()) {
      row.addCell(this.createCells(family, payload));
    }
    return row;
  }

  private void addMetadata(final SinkRecord record, final Map<String, Object> payload) {
    payload.put("created_at", record.timestamp());
    payload.put("processed_at", System.currentTimeMillis());
    payload.put("topic", record.topic());
    payload.put("partition", record.kafkaPartition());
  }

  @VisibleForTesting
  WritableFamilyCells createCells(final String family, final Map<String, ? extends Object> row) {
    final Map<String, Object> filteredRow = this.filterRow(family, row);
    final List<WritableCell> cells = filteredRow.entrySet().stream()
      .map(e -> new WritableCell(TypeUtils.toByteString(e.getKey()),
        TypeUtils.toByteString(e.getValue())))
      .collect(Collectors.toList());
    return new WritableFamilyCells(family, cells);
  }

  @VisibleForTesting
  Map<String, Object> filterRow(final String family, final Map<String, ? extends Object> row) {
    return row.entrySet().stream()
      // TODO(@damon): prevent filtering to allow *all* features to be saved to BT
      // .filter((e) -> this.config.familyQualifiers(family).contains(e.getKey()))
      .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  /**
   * Compute the row key - if a key qualifier is given then compute row key using the record. if
   * not, consider SinkRecord key as the row key. Refer {@link DefaultKeyParser} for more details.
   *
   * @param record
   * @param row
   * @return
   */
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
        "keyQualifiers are not configured and there is no key found in the SinkRecord!");
    }
    return rowKey;
  }
}
