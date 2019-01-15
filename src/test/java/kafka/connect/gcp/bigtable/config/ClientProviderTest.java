package kafka.connect.gcp.bigtable.config;

import org.junit.jupiter.api.Test;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import kafka.connect.gcp.bigtable.Integration;
import kafka.connect.gcp.bigtable.config.AuthConfig;
import kafka.connect.gcp.bigtable.config.ClientProvider;

@Integration
public class ClientProviderTest {

  @Test
  public void shouldGetClient() throws Exception {
    final ServiceAccountCredentials credentials =
        AuthConfig.from("/Users/sanjuthomas/keys/demo-instance-key.json");
    final BigtableDataClient client =
        ClientProvider.provideUsing(credentials, "demo-project", "demo-instance");
    client.close();
  }

}
