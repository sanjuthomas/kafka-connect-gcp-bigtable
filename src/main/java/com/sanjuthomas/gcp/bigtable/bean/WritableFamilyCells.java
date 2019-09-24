package com.sanjuthomas.gcp.bigtable.bean;

import java.util.List;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 * Represent all the cells belongs to a column family in Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
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

  @Override
  public String toString() {
    return "WritableCells [family=" + this.family + ", cells=" + this.cells + "]";
  }
}
