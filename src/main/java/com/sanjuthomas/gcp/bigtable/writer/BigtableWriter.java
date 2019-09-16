package com.sanjuthomas.gcp.bigtable.writer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Result;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableCell;
import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig;

/**
 * Default Bigtable writer implementation.
 * 
 * This implementation write rows using bulkMutateRowsAsync. There is no error handling or retry
 * logic is implemented in the open source version.
 *
 * @author Sanju Thomas
 *
 */
public class BigtableWriter implements Writer<WritableRow, Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(BigtableWriter.class);

  private final List<WritableRow> rows;
  private final BigtableDataClient client;
  private final WriterConfig config;

  public BigtableWriter(final WriterConfig config, final BigtableDataClient client)
      throws FileNotFoundException, IOException {
    this.config = config;
    this.rows = new ArrayList<>();
    this.client = client;
  }

  @Override
  public Result<Boolean> flush() {
    final BulkMutation batch = BulkMutation.create(this.config.table());
    for (final WritableRow row : this.rows) {
      for (final WritableFamilyCells familyCells : row.familyCells()) {
        this.addMutation(batch, row.rowKey(), familyCells.family(), familyCells.cells());
      }
    }
    final boolean executeAsync = this.executeAsync(batch);
    this.rows.clear();
    return () -> executeAsync;
  }

  @VisibleForTesting
  boolean executeAsync(final BulkMutation batchMutation) {
    final ApiFuture<Void> result = this.client.bulkMutateRowsAsync(batchMutation);
    try {
      result.get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
    return result.isDone();
  }

  private void addMutation(final BulkMutation batch, final String rowKey, final String family,
      final List<WritableCell> cells) {
    for (final WritableCell cell : cells) {
      batch.add(rowKey, Mutation.create().setCell(family, cell.qualifier(), cell.value()));
    }
  }

  @Override
  public int buffer(final WritableRow row) {
    this.rows.add(row);
    return this.rows.size();
  }
  

  @Override
  public void close() {
    try {
      this.client.close();
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  public int bufferSize() {
    return rows.size();
  }
}
