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

package com.sanjuthomas.gcp.bigtable.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.TransformerConfig;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;
import com.sanjuthomas.gcp.bigtable.exception.BigtableWriteFailedException;
import com.sanjuthomas.gcp.bigtable.transform.JsonEventTransformer;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

@ExtendWith(MockitoExtension.class)
public class BigtableWriterTest {

  @Mock
  private BigtableDataClient client;

  private BigtableWriter writer;
  private BigtableWriter writerIgnoreError;
  private JsonEventTransformer transformer;
  private final List<String> keyQualifiers = Arrays.asList(new String[]{"symbol"});
  private final List<String> families = Arrays.asList(new String[]{"data", "metadata"});

  @BeforeEach
  public void setUp() throws FileNotFoundException, IOException {
    final Map<String, List<String>> familyToQualifierMapping = new HashMap<>();
    familyToQualifierMapping.put("data", Arrays.asList(new String[]{"symbol", "name", "sector"}));
    familyToQualifierMapping.put("metadata",
      Arrays.asList(new String[]{"create_time", "processing_time", "topic"}));
    final TransformerConfig config =
      new TransformerConfig(this.keyQualifiers, "_", this.families, familyToQualifierMapping);
    this.transformer = new JsonEventTransformer(config);
    final WriterConfig writerConfig = new WriterConfig("/Users/sathomas/keys/demo-key.json",
      "demo-project", "demo-instance", "demo-table", 1024, false);
    writerConfig.setErrorHandlerConfig(new ErrorHandlerConfig(3, 1, true));
    this.writer = new BigtableWriter(writerConfig, client);

    final WriterConfig writerConfigIgnoreError = new WriterConfig(
      "/Users/sathomas/keys/demo-key.json",
      "demo-project", "demo-instance", "demo-table", 1024, true);
    writerConfigIgnoreError.setErrorHandlerConfig(new ErrorHandlerConfig(0, 0, false));
    this.writerIgnoreError = new BigtableWriter(writerConfigIgnoreError, client);
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldWrite(final SinkRecord record) throws InterruptedException, ExecutionException {
    doNothing().when(client).bulkMutateRows(any(BulkMutation.class));
    final WritableRow row = this.transformer.transform(record);
    assertEquals(1, writer.buffer(row));
    assertEquals(1, writer.bufferSize());
    writer.flush();
    assertEquals(0, writer.bufferSize());
    verify(client, times(1)).bulkMutateRows(any(BulkMutation.class));
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldFlushWhenContinueAfterWriteErrorIsSetToTrue(final SinkRecord record)
    throws InterruptedException, ExecutionException {
    doThrow(ApiException.class).when(client).bulkMutateRows(any(BulkMutation.class));
    final WritableRow row = this.transformer.transform(record);
    assertEquals(1, writerIgnoreError.buffer(row));
    writerIgnoreError.flush();
    verify(client, times(1)).bulkMutateRows(any(BulkMutation.class));
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldNotWrite(final SinkRecord record)
    throws InterruptedException, ExecutionException {
    doThrow(ApiException.class).when(client).bulkMutateRows(any(BulkMutation.class));
    final WritableRow row = this.transformer.transform(record);
    assertEquals(1, writer.buffer(row));
    Assertions.assertThrows(BigtableWriteFailedException.class, () -> {
      writer.flush();
    });
    verify(client, times(1)).bulkMutateRows(any(BulkMutation.class));
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldRetryThreeTimes(final SinkRecord record) {
    final ApiException exception =
      new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, true);
    doThrow(exception).when(client).bulkMutateRows(any(BulkMutation.class));
    final WritableRow row = this.transformer.transform(record);
    assertEquals(1, writer.buffer(row));
    Assertions.assertThrows(BigtableWriteFailedException.class, () -> {
      writer.flush();
    });
    verify(client, times(4)).bulkMutateRows(any(BulkMutation.class));
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldWriteSecondTime(final SinkRecord record) {
    final ApiException exception =
      new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, true);
    doThrow(exception).doNothing().when(client).bulkMutateRows(any(BulkMutation.class));
    final WritableRow row = this.transformer.transform(record);
    assertEquals(1, writer.buffer(row));
    writer.flush();
    verify(client, times(2)).bulkMutateRows(any(BulkMutation.class));
  }

  @Test
  public void shouldClose() throws Exception {
    doNothing().when(client).close();
    writer.close();
    assertEquals(1, 1);
  }

}
