package com.sanjuthomas.gcp.bigtable.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.resolvers.WritableRowsResolver;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class PartitionerTest {
  
  private Partitioner partitioner;
  private Partitioner smallPartitioner;
  
  @BeforeEach
  public void setUp() {
    partitioner = new Partitioner(10);
    smallPartitioner = new Partitioner(1);
  }
  
  @Test
  public void testPreconditions() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      partitioner.partitions(null);
    });
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      partitioner.partitions(new ArrayList<>());
    });
    assertEquals(1, 1);
  }
  
  @Test
  public void shouldReturnSizeOfPartition() {
    assertEquals(10, partitioner.partitionsCount(11));
    assertEquals(10, partitioner.partitionsCount(20));
    assertEquals(8, partitioner.partitionsCount(8));
    assertEquals(1, partitioner.partitionsCount(1));
  }
  
  @Test
  @ExtendWith(WritableRowsResolver.class)
  public void shouldGetBatches(final List<WritableRow> rows) {
    assertEquals(2, rows.size());
    assertEquals(2, smallPartitioner.partitions(rows).size());
    assertEquals(1, partitioner.partitions(rows).size());
  }
}
