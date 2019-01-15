package bigtable.utils;

import java.io.File;
import java.io.FileInputStream;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;

public class QueryTable {
	
	private static final String KEY_FILE = "/home/keys/demo-instance.json";

	public void execute(String project, String instance, String table) throws Exception {
		final GoogleCredentials credentials;
		File credentialsPath = new File(KEY_FILE);
		try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
			credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
		}
		BigtableDataSettings bigtableDataSettings = BigtableDataSettings.newBuilder()
				.setInstanceName(InstanceName.of(project, instance))
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
		try (BigtableDataClient bigtableDataClient = BigtableDataClient.create(bigtableDataSettings)) {

			Query query = Query.create(table);
			ServerStream<Row> readRows = bigtableDataClient.readRows(query);
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
				System.exit(0);
			});
		}
	}
	
	public static void main(String[] args) throws Exception {
		new QueryTable().execute("demo-project", "demo-instance", "demo-table");
	}
}

