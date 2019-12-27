/*
 * Copyright (c) 2019 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

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
            "demo-project", "demo-instance", "demo-table", 1024, false));
  }

  @Test
  public void shouldGetClient() throws Exception {
    final BigtableDataClient bigtableClient = clientProvider.client();
    bigtableClient.close();
    assertEquals(1, 1);
  }

}
