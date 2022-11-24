/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.bigtable.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher.BulkMutationFailure;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class UpsertData {

  private static final String KEY_FILE = "/Users/sanjuthomas/keys/demo-instance-key.json";

  public void execute(final String project, final String instance, final String table,
      final Map<String, Object> data) throws Exception {
    final GoogleCredentials credentials;
    final File credentialsPath = new File(KEY_FILE);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
    final BigtableDataSettings bigtableDataSettings =
        BigtableDataSettings.newBuilder().setInstanceId(instance).setProjectId(project)
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
    try (BigtableDataClient bigtableDataClient = BigtableDataClient.create(bigtableDataSettings)) {
      final BulkMutation batch = BulkMutation.create(table);
      data.entrySet().forEach(e -> {
        batch.add("NYSE-APPL",
            Mutation.create().setCell("data", e.getKey(), Objects.toString(e.getValue())));
      });
      batch.add("metadata",
          Mutation.create().setCell("metadata", "created_at", Objects.toString(new Date())));
      batch.add("metadata",
          Mutation.create().setCell("metadata", "processed_at", Objects.toString(new Date())));
      batch.add("metadata", Mutation.create().setCell("metadata", "topic", "demo-topic"));
      batch.add("metadata", Mutation.create().setCell("metadata", "partition", "0"));

      final ApiFuture<Void> result = bigtableDataClient.bulkMutateRowsAsync(batch);

      ApiFutures.addCallback(result, new ApiFutureCallback<Void>() {
        public void onFailure(Throwable t) {
          if (t instanceof BulkMutationFailure) {
            System.out.println("Some entries failed to apply");
          } else {
            t.printStackTrace();
          }
        }

        public void onSuccess(Void ignored) {
          System.out.println("Successfully applied all mutation");
        }
      }, MoreExecutors.directExecutor());

      result.get();
    }
  }

  public static void main(final String[] args) throws Exception {
    final Map<String, Object> data = new HashMap<>();
    data.put("client", "c-100");
    data.put("exchange", "NYSE");
    data.put("symbol", "AAPL");
    data.put("price", 201.12);
    data.put("quantity", 1200);
    new UpsertData().execute("civic-athlete-251623", "demo-instance", "demo-table", data);
  }
}
