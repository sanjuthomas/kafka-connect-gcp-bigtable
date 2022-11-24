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

import java.util.List;
import com.google.common.annotations.VisibleForTesting;
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

  public List<List<WritableRow>> partitions(final List<WritableRow> rows) {
    Preconditions.checkArgument(!(rows == null || rows.size() == 0), "argument rows can't be null or empty");
    return Lists.partition(rows, partitionsCount(rows.size()));
  }

  @VisibleForTesting
  int partitionsCount(final int totalRows) {
   if (totalRows > bulkMutateRowsMaxSize) {
      return bulkMutateRowsMaxSize;
    }
    return totalRows;
  }
}