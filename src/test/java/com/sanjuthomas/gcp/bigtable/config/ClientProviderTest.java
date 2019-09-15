package com.sanjuthomas.gcp.bigtable.config;

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
  public void setup() {
    clientProvider = new ClientProvider(
        new WriterConfig("/Users/sanjuthomas/keys/civic-athlete-251623-92f3165c768b.json",
            "demo-project", "demo-instance", "demo-table"));
  }

  @Test
  public void shouldGetClient() throws Exception {
    final BigtableDataClient bigtableClient = clientProvider.client();
    bigtableClient.close();
  }

}
