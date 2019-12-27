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

package com.sanjuthomas.bigtable.utils;

import java.io.File;
import java.io.FileInputStream;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;

/**
 *
 * @author Sanju Thomas
 *
 */
public class QueryTable {

  private static final String KEY_FILE = "/Users/sanjuthomas/keys/demo-instance-key.json";

  public void execute(final String project, final String instance, final String table)
      throws Exception {
    final GoogleCredentials credentials;
    final File credentialsPath = new File(KEY_FILE);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
    final BigtableDataSettings bigtableDataSettings =
        BigtableDataSettings.newBuilder().setProjectId(project).setInstanceId(instance)
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    try (BigtableDataClient bigtableDataClient = BigtableDataClient.create(bigtableDataSettings)) {

      final Query query = Query.create(table);
      final ServerStream<Row> readRows = bigtableDataClient.readRows(query);
      readRows.forEach(r -> {
        System.out.println("------------------------------------------------");
        System.out.println("------------");
        System.out.println(new String(r.getKey().toByteArray()));
        System.out.println("------------");
        r.getCells().forEach(c -> {
          System.out.println(c.getFamily());
          System.out.println(new String(c.getQualifier().toByteArray()));
          System.out.println(new String(c.getValue().toByteArray()));
          System.out.println(c.getTimestamp());
        });
        System.out.println("------------------------------------------------");
      });
    }
  }

  public static void main(final String[] args) throws Exception {
    new QueryTable().execute("civic-athlete-251623", "demo-instance", "demo-table");
  }
}

