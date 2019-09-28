package com.sanjuthomas.gcp.bigtable.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;

/**
 * 
 * The class responsible for creating the connection/bigtable client. Creation of the client is an
 * expensive process so we cache the client. Upon a write error the client is closed and removed
 * from the cache.
 * 
 * Refer {@link WriterProvider} to know how it is cached.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class ClientProvider {

  private static final Logger logger = LoggerFactory.getLogger(ClientProvider.class);
  private WriterConfig writerConfig;

  public ClientProvider(final WriterConfig writerConfig) {
    logger.info("ClientProvider is created by task id {}", Thread.currentThread().getId());
    this.writerConfig = writerConfig;
  }

  /**
   * Create a BigtableDataClient using the given WriterConfig.
   * 
   * @return BigtableDataClient
   * @throws IOException
   */
  public BigtableDataClient client() throws IOException {
    logger.info("BigtableDataClient is created for task {}", Thread.currentThread().getId());
    final BigtableDataSettings settings = BigtableDataSettings.newBuilder()
        .setProjectId(this.writerConfig.project()).setInstanceId(this.writerConfig.instance())
        .setCredentialsProvider(FixedCredentialsProvider.create(credential())).build();
    return BigtableDataClient.create(settings);
  }

  private ServiceAccountCredentials credential() throws FileNotFoundException, IOException {
    final File credentialFile = new File(writerConfig.keyFile());
    try (FileInputStream seviceAccountStream = new FileInputStream(credentialFile)) {
      return ServiceAccountCredentials.fromStream(seviceAccountStream);
    }
  }
}
