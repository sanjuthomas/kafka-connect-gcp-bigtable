package com.sanjuthomas.gcp.bigtable.writer;

import java.util.List;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;

/**
 * 
 * Construct micro batches as per the configured size.
 * 
 * @author Sanju Thomas
 *
 */
public class Partitioner {

  private final int bulkMutateRowsMaxSize;

  Partitioner(final int bulkMutateRowsMaxSize) {
    this.bulkMutateRowsMaxSize = bulkMutateRowsMaxSize;
  }

  public List<List<WritableRow>> batches(final List<WritableRow> rows) {
    Preconditions.checkArgument(!(rows == null || rows.size() == 0),
        "argument rows can't be null or empty");
    return Lists.partition(rows, partitionsCount(rows.size()));
  }

  int partitionsCount(final int totalRows) {
   if (totalRows > bulkMutateRowsMaxSize) {
      return bulkMutateRowsMaxSize;
    }
    return totalRows;
  }

}
