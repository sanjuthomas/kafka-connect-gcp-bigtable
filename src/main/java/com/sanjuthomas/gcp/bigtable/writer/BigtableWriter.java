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

package com.sanjuthomas.gcp.bigtable.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
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
 * there would be one writer per topic and every task thread will get it's own writer instance.
 * <p>
 * Nothing is shared among tasks. A task gets a topic to read from and a writer to flush the
 * messages out.
 * <p>
 * This implementation write rows using bulkMutateRows.
 * <p>
 * Refer {@link ErrorHandler} for more details about error handling and retry logic.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 */
@Evolving
@Slf4j
public class BigtableWriter implements Writer<WritableRow, Boolean> {

  private final List<WritableRow> rows;
  private final BigtableDataClient client;
  private final WriterConfig config;
  private final ErrorHandler errorHandler;
  private final Partitioner partitioner;
  private final boolean continueAfterWriteError;

  public BigtableWriter(final WriterConfig config, final BigtableDataClient client) {
    this.config = config;
    this.rows = new ArrayList<>();
    this.client = client;
    this.errorHandler = new ErrorHandler(config.getErrorHandlerConfig());
    this.partitioner = new Partitioner(config.bulkMutateRowsMaxSize());
    this.continueAfterWriteError = config.continueAfterWriteError();
  }

  @Override
  public void flush() {
    final List<List<WritableRow>> partitions = partitioner.partitions(this.rows);
    try {
      for (final List<WritableRow> partition : partitions) {
        flush(partition);
      }
    } finally {
      this.rows.clear();
    }
  }

  @VisibleForTesting
  void flush(final List<WritableRow> rows) {
    log.debug("Flushing {} rows", rows.size());
    final BulkMutation batch = BulkMutation.create(this.config.table());
    for (final WritableRow row : rows) {
      for (final WritableFamilyCells familyCells : row.familyCells()) {
        this.addMutation(batch, row.rowKey(), familyCells.family(), familyCells.cells());
      }
    }
    try {
      this.execute(batch);
      log.debug("{} rows written to Bigtable", rows.size());
    } catch (final BigtableWriteFailedException e) {
      if (!continueAfterWriteError) {
        throw e;
      }
      log.error("continueAfterWriteError is configured as {} so continuing to next batch.",
        continueAfterWriteError);
      log.info("batch write failed. batch count was {}", rows.size());
    } finally {
      errorHandler.reset();
    }
  }

  /**
   * Execute the BulkMutation
   *
   * @param bulkMutation
   * @throws InterruptedException
   */
  @VisibleForTesting
  void execute(final BulkMutation bulkMutation) {
    try {
      this.client.bulkMutateRows(bulkMutation);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      if (!execute(bulkMutation, e)) {
        throw new BigtableWriteFailedException(
          String.format("Failed to save the batch to Bigtable and batch size was %s", rows.size()));
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
      } catch (final Exception e) {
        log.error("Write failed due to {}. retry attempts {}", e.getMessage(), result.attempt(),
          e);
        result = errorHandler.handle(exception);
      }
    }
    return false;
  }

  private void addMutation(final BulkMutation batch, final String rowKey, final String family,
    final List<WritableCell> cells) {
    for (final WritableCell cell : cells) {
      log.debug("Adding cell for row key {}. family {}, cell qualifier {}, cell value {}", rowKey,
        family, cell.qualifier(), cell.value());
      batch.add(rowKey, Mutation.create().setCell(family, cell.qualifier(), cell.value()));
    }
  }

  @Override
  public int buffer(final WritableRow row) {
    log.debug("Buffering {}", row);
    this.rows.add(row);
    return this.rows.size();
  }

  @Override
  public void close() {
    try {
      this.client.close();
    } catch (final Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public int bufferSize() {
    return rows.size();
  }
}