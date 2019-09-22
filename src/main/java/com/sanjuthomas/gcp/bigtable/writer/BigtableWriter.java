package com.sanjuthomas.gcp.bigtable.writer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.common.annotations.VisibleForTesting;
import com.sanjuthomas.gcp.bigtable.Writer;
import com.sanjuthomas.gcp.bigtable.bean.WritableCell;
import com.sanjuthomas.gcp.bigtable.bean.WritableFamilyCells;
import com.sanjuthomas.gcp.bigtable.bean.WritableRow;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig;
import com.sanjuthomas.gcp.bigtable.exception.BigtableWriteFailedException;
import com.sanjuthomas.gcp.bigtable.writer.ErrorHandler.Result;

/**
 * Default Bigtable writer implementation and this class is not thread safe. As per the design,
 * there would be one writer per topic and every task thread will have it's on writer instance.
 * 
 * a Task Thread -> a Topic -> a Writer
 * 
 * This implementation write rows using bulkMutateRows.
 * 
 * @see ErrorHandler for more details about error handling and retry logic.
 *
 * @author Sanju Thomas
 *
 */
public class BigtableWriter implements Writer<WritableRow, Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(BigtableWriter.class);

  private final List<WritableRow> rows;
  private final BigtableDataClient client;
  private final WriterConfig config;
  private final ErrorHandler errorHandler;

  public BigtableWriter(final WriterConfig config, final BigtableDataClient client)
      throws FileNotFoundException, IOException {
    this.config = config;
    this.rows = new ArrayList<>();
    this.client = client;
    this.errorHandler = new ErrorHandler(config.getErrorHandlerConfig());
  }

  @Override
  public void flush() {
    final BulkMutation batch = BulkMutation.create(this.config.table());
    for (final WritableRow row : this.rows) {
      for (final WritableFamilyCells familyCells : row.familyCells()) {
        this.addMutation(batch, row.rowKey(), familyCells.family(), familyCells.cells());
      }
    }
    try {
      this.execute(batch);
    } catch (Exception e) {
      throw e;
    } finally {
      this.rows.clear();
      errorHandler.reset();
    }
  }

  /**
   * Execute the BulkMutation
   * 
   * @param batchMutation
   * @throws InterruptedException
   */
  @VisibleForTesting
  void execute(final BulkMutation bulkMutation) {
    try {
      this.client.bulkMutateRows(bulkMutation);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      if (!execute(bulkMutation, e)) {
        throw new BigtableWriteFailedException(String
            .format("Failed to save the batch to Bigtable and batch size was %s", rows.size()));
      }
    }
  }

  @VisibleForTesting
  boolean execute(final BulkMutation bulkMutation, final Exception exception) {
    Result result = errorHandler.handle(exception);
    while (result.retry()) {
      try {
        TimeUnit.SECONDS.sleep(result.secondsToSleep());
        this.client.bulkMutateRows(bulkMutation);
        return true;
      } catch (Exception e) {
        logger.error("Write failed due to {}. retry attemps {}", e.getMessage(), result.attempt(),
            e);
        result = errorHandler.handle(exception);
      }
    }
    return false;
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
