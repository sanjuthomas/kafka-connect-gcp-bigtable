package com.sanjuthomas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.sanjuthomas.gcp.bigtable.Integration;

/**
 * 
 * @author Sanju Thomas
 *
 */
@Integration
public class ClientProviderTest {

  private ClientProvider clientProvider;

  @BeforeEach
  public void setUp() {
    clientProvider = new ClientProvider(
        new WriterConfig("/Users/sanjuthomas/keys/civic-athlete-251623-e16dce095204.json",
            "demo-project", "demo-instance", "demo-table", 1024));
  }

  @Test
  public void shouldGetClient() throws Exception {
    final BigtableDataClient bigtableClient = clientProvider.client();
    bigtableClient.close();
    assertEquals(1, 1);
  }

}
