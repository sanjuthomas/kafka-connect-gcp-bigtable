package kafka.connect.gcp.bigtable.config;

import java.io.IOException;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.InstanceName;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ClientProvider {

  public static BigtableDataClient provideUsing(final ServiceAccountCredentials credentials,
      final String project, final String instance) throws IOException {
    final BigtableDataSettings settings =
        BigtableDataSettings.newBuilder().setInstanceName(InstanceName.of(project, instance))
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    return BigtableDataClient.create(settings);
  }
}
