package com.sanjuthomas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.sanjuthomas.gcp.bigtable.Integration;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;

/**
 * 
 * @author Sanju Thomas
 *
 */
@Integration
public class WriterProviderTest {
  
  private WriterProvider writerProvider;
  
  @BeforeEach
  public void setup() {
    final ConfigProvider configProvider = new ConfigProvider();
    configProvider.load("src/test/resources/", "demo-topic");
    writerProvider = new WriterProvider(configProvider);
  }
  
  @Test
  public void shouldGetWriter() {
    Writer<WritableRow, Boolean> writer = writerProvider.writer("demo-topic");
    Writer<WritableRow, Boolean> writer1 = writerProvider.writer("demo-topic");
    assertEquals(writer.hashCode(), writer1.hashCode());
    writer.close();
  }

}
