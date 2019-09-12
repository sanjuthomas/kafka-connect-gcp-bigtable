package com.sanjuthoas.gcp.bigtable.config;

import java.io.IOException;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ClientProvider {

  public static BigtableDataClient provideUsing(final ServiceAccountCredentials credentials,
      final String project, final String instance) throws IOException {
    final BigtableDataSettings settings =
        BigtableDataSettings.newBuilder().setProjectId(project).setInstanceId(instance)
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    return BigtableDataClient.create(settings);
  }
}
