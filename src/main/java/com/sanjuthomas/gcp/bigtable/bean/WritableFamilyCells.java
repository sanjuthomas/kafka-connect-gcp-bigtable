package com.sanjuthomas.gcp.bigtable.bean;

import java.util.List;

/**
 *
 * @author Sanju Thomas
 *
 */
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
