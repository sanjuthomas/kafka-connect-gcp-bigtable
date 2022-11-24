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

package com.sanjuthomas.gcp.bigtable.bean;

import java.util.List;
import lombok.ToString;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 * 
 * Represent all the cells belongs to a column family in Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
@ToString
public class WritableFamilyCells {

  private final String family;
  private final List<WritableCell> cells;

  public WritableFamilyCells(final String family, final List<WritableCell> cells) {
    this.family = family;
    this.cells = cells;
  }

  public List<WritableCell> cells() {
    return this.cells;
  }

  public String family() {
    return this.family;
  }
}