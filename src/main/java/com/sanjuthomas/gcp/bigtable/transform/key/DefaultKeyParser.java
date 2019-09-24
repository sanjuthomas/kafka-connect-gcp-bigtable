package com.sanjuthomas.gcp.bigtable.transform.key;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.apache.kafka.connect.sink.SinkRecord;
import com.sanjuthomas.gcp.bigtable.Parser;

/**
 *
 * @author Sanju Thomas
 *  @since 1.0.3
 *
 */
@Stable
public class DefaultKeyParser implements Parser<Object, String> {

  private final List<String> keyQualifies;
  private final String keyDelimiter;

  public DefaultKeyParser(final List<String> keyQualifies, final String keyDelimiter) {
    this.keyQualifies = keyQualifies;
    this.keyDelimiter = keyDelimiter;
  }

 /**
  * Extract key from the record.
  */
  @Override
  public String parse(final Object record) {
    if (record instanceof SinkRecord) {
      return this.getKeyFromSinkRecrod(record);
    } else if (record instanceof Map) {
      return this.getKeyFromMapEvent(record);
    }
    throw new IllegalArgumentException("Unknown type " + record.getClass().getName());
  }

  private String getKeyFromMapEvent(final Object record) {
    final Map<?, ?> row = (Map<?, ?>) record;
    return this.keyQualifies.stream().map(kq -> String.valueOf(row.get(kq)))
        .collect(Collectors.joining(this.keyDelimiter));
  }

  private String getKeyFromSinkRecrod(final Object record) {
    final SinkRecord sinkRecord = (SinkRecord) record;
    return (String) sinkRecord.key();
  }
}
