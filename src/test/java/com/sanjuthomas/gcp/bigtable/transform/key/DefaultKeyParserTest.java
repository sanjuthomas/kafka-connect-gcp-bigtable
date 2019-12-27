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

package com.sanjuthomas.gcp.bigtable.transform.key;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.gcp.resolvers.KeylessSinkRecordResolver;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 *
 * @author Sanju Thomas
 *
 */
public class DefaultKeyParserTest {

  private DefaultKeyParser keyParser;
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeEach
  public void setUp() {
    this.keyParser = new DefaultKeyParser(Arrays.asList(new String[] {"symbol"}), "_");
  }

  @Test
  @ExtendWith(KeylessSinkRecordResolver.class)
  public void shouldGetNullKey(final SinkRecord record) {
    assertNull(this.keyParser.parse(record));
  }

  @Test
  public void shouldGetNullKey() {
    assertThrows(IllegalArgumentException.class, () -> {
      assertNull(this.keyParser.parse("fake"));
    });
  }

  @Test
  @ExtendWith(KeylessSinkRecordResolver.class)
  public void shouldGetKey(final SinkRecord record) {
    assertEquals("MMM", this.keyParser.parse(
        OBJECT_MAPPER.convertValue(record.value(), new TypeReference<Map<String, Object>>() {})));
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldGetKeyForSinkRecord(final SinkRecord record) {
    assertEquals("MMM", this.keyParser.parse(record));
  }

}
