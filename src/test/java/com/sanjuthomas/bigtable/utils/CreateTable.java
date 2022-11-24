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

import java.io.IOException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;

/**
 * 
 * @author Sanju Thomas
 *
 */
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
    new CreateTable().execute("civic-athlete-251623", "demo-instance", "demo-table");
  }
}
