package com.sanjuthomas.gcp.bigtable.config;

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
    writer.close();
  }

}
