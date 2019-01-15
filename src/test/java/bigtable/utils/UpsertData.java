package bigtable.utils;

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
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Mutation;

public class UpsertData {
	
	private static final String KEY_FILE = "/home/keys/demo-instance.json";
	public void execute(String project, String instance, String table, Map<String, String> data) throws Exception {
		final GoogleCredentials credentials;
		File credentialsPath = new File( KEY_FILE);
		try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
			credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		}
		BigtableDataSettings bigtableDataSettings = BigtableDataSettings.newBuilder()
				.setInstanceName(InstanceName.of(project, instance))
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
		try (BigtableDataClient bigtableDataClient = BigtableDataClient.create(bigtableDataSettings)) {
			BulkMutation batch = BulkMutation.create(table);
			data.entrySet().forEach(e -> {
				batch.add("rowkey1", Mutation.create().setCell("family1", e.getKey(), e.getValue()));
				batch.add("rowkey1", Mutation.create().setCell("family2", e.getKey(), e.getValue()));
			});
			ApiFuture<Void> result = bigtableDataClient.bulkMutateRowsAsync(batch);
			result.get();
			System.out.println(result.isDone());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, String> data = new HashMap<>();
		data.put("key1", "value1");
		data.put("key2", "value2");
		data.put("key3", "value3");
		new UpsertData().execute("demo-projecct", "demo-instane", "demo-table", data);
	}
}
