package com.sanjuthoas.gcp.bigtable.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.sanjuthoas.gcp.bigtable.Integration;

@Integration
public class AuthConfigTest {

  @Test
  public void shouldGetAuthConfig() throws FileNotFoundException, IOException {
    final ServiceAccountCredentials credentials =
        AuthConfig.from("/Users/sanjuthomas/keys/demo-instance-key.json");
    assertEquals("primeval-jet-227401", credentials.getProjectId());
  }
}
