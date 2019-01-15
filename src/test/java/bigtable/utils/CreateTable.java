package bigtable.utils;

import java.io.IOException;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;

public class CreateTable {
	
	public void execute(String project, String instance, String table) throws IOException {
		BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
				.setInstanceName(com.google.bigtable.admin.v2.InstanceName.of(project, instance))
				.build();
		try (BigtableTableAdminClient bigtableDataClient = BigtableTableAdminClient.create(adminSettings)) {
			CreateTableRequest request = CreateTableRequest.of(table);
			bigtableDataClient.createTable(request);
		}
	}
	
	//have the GOOGLE_APPLICATION_CREDENTIALS system environment variable set
	public static void main(String[] args) throws IOException {
		new CreateTable().execute("pcln-pl-cps-poc", "demo-instance", "demo-table");
	}
}
