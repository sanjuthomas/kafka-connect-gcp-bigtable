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

package com.sanjuthomas.gcp.bigtable.bean;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *
 * Represent a row in the Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class WritableRow {

  private final String rowKey;
  private List<WritableFamilyCells> familyCells;

  public WritableRow(final String rowKey) {
    this.rowKey = rowKey;
  }

  public void addCell(final WritableFamilyCells cell) {
    if(null == familyCells) {
      familyCells = new ArrayList<>();
    }
    this.familyCells.add(cell);
  }

  public String rowKey() {
    return this.rowKey;
  }

  public List<WritableFamilyCells> familyCells() {
    return this.familyCells;
  }

  @Override
  public String toString() {
    return "WritableRow [rowKey=" + this.rowKey + ", familyCells=" + this.familyCells + "]";
  }
}
