package com.sanjuthomas.bigtable.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class UpsertData {

  private static final String KEY_FILE = "/Users/sanjuthomas/keys/demo-instance-key.json";

  public void execute(final String project, final String instance, final String table,
      final Map<String, String> data) throws Exception {
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
        batch.add("rowkey1", Mutation.create().setCell("data", e.getKey(), e.getValue()));
        batch.add("rowkey1", Mutation.create().setCell("metadata", e.getKey(), e.getValue()));
      });
      final ApiFuture<Void> result = bigtableDataClient.bulkMutateRowsAsync(batch);
      result.get();
      System.out.println(result.isDone());
    }
  }

  public static void main(final String[] args) throws Exception {
    final Map<String, String> data = new HashMap<>();
    data.put("key1", "value1");
    data.put("key2", "value2");
    data.put("key3", "value3");
    new UpsertData().execute("primeval-jet-227401", "demo-instane", "demo-table", data);
  }
}
