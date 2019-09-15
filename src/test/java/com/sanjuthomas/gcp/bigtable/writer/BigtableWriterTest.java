package com.sanjuthomas.gcp.bigtable.writer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.TransformerConfig;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig;
import com.sanjuthomas.gcp.bigtable.transform.JsonEventTransformer;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

@ExtendWith(MockitoExtension.class)
public class BigtableWriterTest {

  @Mock
  private BigtableDataClient client;
  
  @Mock
  private ApiFuture<Void> apiFuture;
  
  private BigtableWriter writer;
  private JsonEventTransformer transformer;
  private final List<String> keyQualifiers = Arrays.asList(new String[] {"symbol"});
  private final List<String> families = Arrays.asList(new String[] {"data", "metadata"});
  private Map<String, List<String>> familyToQualifierMapping;

  @BeforeEach
  public void setup() throws FileNotFoundException, IOException {
    this.familyToQualifierMapping = new HashMap<>();
    this.familyToQualifierMapping.put("data",
        Arrays.asList(new String[] {"symbol", "name", "sector"}));
    this.familyToQualifierMapping.put("metadata",
        Arrays.asList(new String[] {"create_time", "processing_time", "topic"}));
    final TransformerConfig config = new TransformerConfig(this.keyQualifiers, "_", this.families,
        this.familyToQualifierMapping);
    this.transformer = new JsonEventTransformer(config);
    WriterConfig writerConfig = new WriterConfig("/Users/sathomas/keys/demo-key.json",
        "demo-project", "demo-instance", "demo-table");
    this.writer = new BigtableWriter(writerConfig, client);
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldWrite(final SinkRecord record) throws InterruptedException, ExecutionException {
    when(client.bulkMutateRowsAsync(any(BulkMutation.class))).thenReturn(apiFuture);
    when(apiFuture.isDone()).thenReturn(true);
    final WritableRow row = this.transformer.transform(record);
    writer.buffer(row);
    assertTrue(writer.flush().get());
    verify(client, times(1)).bulkMutateRowsAsync(any(BulkMutation.class));
    verify(apiFuture, times(1)).get();
  }
  
  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldNotWrite(final SinkRecord record) throws InterruptedException, ExecutionException {
    when(client.bulkMutateRowsAsync(any(BulkMutation.class))).thenReturn(apiFuture);
    when(apiFuture.get()).thenThrow(ExecutionException.class);
    final WritableRow row = this.transformer.transform(record);
    writer.buffer(row);
    assertFalse(writer.flush().get());
    verify(client, times(1)).bulkMutateRowsAsync(any(BulkMutation.class));
    verify(apiFuture, times(1)).get();
  }
  
  @Test
  public void shouldClose() throws Exception {
    doNothing().when(client).close();
    writer.close();
  }
  
}
