package kafka.connect.gcp.bigtable.transform.key;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import kafka.connect.gcp.bigtable.Parser;

/**
 *
 * @author Sanju Thomas
 *
 */
public class DefaultKeyParser implements Parser<Object, String> {

    private final List<String> keyQualifies;
    private final String keyDelimiter;

    public DefaultKeyParser(final List<String> keyQualifies, final String keyDelimiter) {
        this.keyQualifies = keyQualifies;
        this.keyDelimiter = keyDelimiter;
    }

    @Override
    public String parse(final Object record) {
        if (record instanceof SinkRecord) {
            return this.getKeyFromSinkRecrod(record);
        } else if (record instanceof Map) {
            return getKeyFromMapEvent(record);
        }
        throw new IllegalArgumentException("Unknown type " + record.getClass().getName());
    }

    private String getKeyFromMapEvent(final Object record) {
        final Map<?, ?> row = (Map<?, ?>) record;
        return this.keyQualifies.stream().map(kq -> String.valueOf(row.get(kq))).collect(Collectors.joining(this.keyDelimiter));
    }

    private String getKeyFromSinkRecrod(final Object record) {
        final SinkRecord sinkRecord = (SinkRecord) record;
        return (String) sinkRecord.key();
    }

}
