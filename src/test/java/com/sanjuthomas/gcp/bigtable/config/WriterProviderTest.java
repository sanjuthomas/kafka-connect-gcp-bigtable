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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.sanjuthomas.gcp.bigtable.Integration;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.exception.BigtableSinkInitializationException;

/**
 *
 * @author Sanju Thomas
 *
 */
@Integration
public class WriterProviderTest {

  private WriterProvider writerProvider;

  @BeforeEach
  public void setUp() {
    final ConfigProvider configProvider = new ConfigProvider();
    configProvider.load("src/test/resources/", "demo-topic");
    writerProvider = new WriterProvider(configProvider);
  }

  @Test
  public void shouldGetWriter() {
    Writer<WritableRow, Boolean> writer = writerProvider.writer("demo-topic");
    Writer<WritableRow, Boolean> writer1 = writerProvider.writer("demo-topic");
    assertEquals(writer.hashCode(), writer1.hashCode());
    writer.close();
    assertEquals(1, 1);
  }

  @Test
  public void shouldNotInitWiter() {
    Assertions.assertThrows(BigtableSinkInitializationException.class, () -> {
      writerProvider.writer("fake-topic-test");
    });
    assertEquals(1, 1);
  }

  @Test
  public void shouldNotInitConfig() {
    final ConfigProvider configProvider = new ConfigProvider();
    Assertions.assertThrows(BigtableSinkInitializationException.class, () -> {
      configProvider.load("src/test/resources/test", "demo-topic");
    });
    assertEquals(1, 1);
  }
}
