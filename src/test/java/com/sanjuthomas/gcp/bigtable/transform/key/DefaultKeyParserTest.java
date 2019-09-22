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
