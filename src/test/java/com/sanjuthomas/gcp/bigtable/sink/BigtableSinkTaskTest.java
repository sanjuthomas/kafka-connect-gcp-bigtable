package com.sanjuthomas.gcp.bigtable.sink;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.WriterProvider;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 * 
 * @author Sanju Thomas
 *
 */

@ExtendWith(MockitoExtension.class)
public class BigtableSinkTaskTest {

  @Mock
  private WriterProvider writerProvider;

  @Mock
  private Writer<WritableRow, Boolean> writer;
  
  private BigtableSinkTask task;
  
  private Collection<TopicPartition> topicPartitions;


  @BeforeEach
  public void setUp() {
    task = new BigtableSinkTask();
    topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition("demo-topic", 0));
    final Map<String, String> configs = new HashMap<>();
    configs.put(BigtableSinkConfig.TOPICS, "demo-topic");
    configs.put(BigtableSinkConfig.CONFIG_FILE_LOCATION, "src/test/resources/");
    task.start(configs);
  }

  @Test
  @ExtendWith(SinkRecordResolver.class)
  public void shouldPut(final SinkRecord record) {
    task.writerProvider = writerProvider;
    when(writerProvider.writer("demo-topic")).thenReturn(writer);
    when(writer.buffer(any(WritableRow.class))).thenReturn(1);
    when(writer.bufferSize()).thenReturn(1);
    task.open(topicPartitions);
    task.put(Arrays.asList(record));
    verify(writerProvider, times(2)).writer("demo-topic");
    verify(writer, times(1)).buffer(any(WritableRow.class));
    verify(writer, times(1)).bufferSize();
    verify(writer, times(1)).flush();
  }
}
