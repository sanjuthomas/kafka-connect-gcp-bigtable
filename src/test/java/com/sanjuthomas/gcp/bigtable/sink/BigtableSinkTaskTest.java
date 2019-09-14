package com.sanjuthomas.gcp.bigtable.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.sanjuthomas.gcp.bigtable.Integration;
import com.sanjuthomas.gcp.resolvers.SinkRecordResolver;

/**
 * 
 * @author Sanju Thomas
 *
 */
@Integration
public class BigtableSinkTaskTest {
	
	private BigtableSinkTask task;
	
	
	@BeforeEach
	public void setup() {
		task = new BigtableSinkTask();
		final Map<String, String> configs = new HashMap<>();
		configs.put(BigtableSinkConfig.TOPICS, "demo-topic");
		configs.put(BigtableSinkConfig.CONFIG_FILE_LOCATION, "src/test/resources/");
		task.start(configs);
	}

	@Test
	@ExtendWith(SinkRecordResolver.class)
	public void shouldPut(final SinkRecord record) {
	
		task.put(Arrays.asList(record));
	}
}
