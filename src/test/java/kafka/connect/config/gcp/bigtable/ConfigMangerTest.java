package kafka.connect.config.gcp.bigtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import kafka.connect.gcp.bigtable.Transformer;
import kafka.connect.gcp.bigtable.Writer;
import kafka.connect.gcp.bigtable.bean.WritableCells;
import kafka.connect.gcp.bigtable.bean.WritableRow;
import kafka.connect.gcp.resolvers.SinkRecordResolver;

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
    final List<WritableCells> cells = row.cells();
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
