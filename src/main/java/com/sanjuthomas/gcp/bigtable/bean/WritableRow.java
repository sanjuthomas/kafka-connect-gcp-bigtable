package com.sanjuthomas.gcp.bigtable.bean;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *  Represent a row in the Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class WritableRow {

  private final String rowKey;
  private final List<WritableFamilyCells> familyCells;

  public WritableRow(final String rowKey) {
    this.rowKey = rowKey;
    this.familyCells = new ArrayList<>();
  }

  public void addCell(final WritableFamilyCells cell) {
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
