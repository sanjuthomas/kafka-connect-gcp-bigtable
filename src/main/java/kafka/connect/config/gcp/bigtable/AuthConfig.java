package kafka.connect.config.gcp.bigtable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.google.auth.oauth2.ServiceAccountCredentials;

/**
 *
 * @author Sanju Thomas
 *
 */
public class AuthConfig {

  public static ServiceAccountCredentials of(final String keyFile)
      throws FileNotFoundException, IOException {
    final File credentialFile = new File(keyFile);
    try (FileInputStream seviceAccountStream = new FileInputStream(credentialFile)) {
      return ServiceAccountCredentials.fromStream(seviceAccountStream);
    }
  }
}
