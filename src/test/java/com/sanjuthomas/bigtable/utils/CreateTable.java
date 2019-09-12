package com.sanjuthomas.bigtable.utils;

import java.io.IOException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;

public class CreateTable {

  public void execute(final String project, final String instance, final String table)
      throws IOException {
    final BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
        .setProjectId(project).setInstanceId(instance).build();
    try (BigtableTableAdminClient bigtableDataClient =
        BigtableTableAdminClient.create(adminSettings)) {
      final CreateTableRequest request =
          CreateTableRequest.of(table).addFamily("data").addFamily("metadata");
      bigtableDataClient.createTable(request);
    }
  }

  // have the GOOGLE_APPLICATION_CREDENTIALS system environment variable set
  public static void main(final String[] args) throws IOException {
    new CreateTable().execute("primeval-jet-227401", "demo-instance", "demo-table");
  }
}
