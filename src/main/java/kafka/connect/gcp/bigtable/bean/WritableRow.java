package kafka.connect.gcp.bigtable.bean;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Sanju Thomas
 *
 */
public class WritableRow {

  private final String rowKey;
  private final List<WritableCells> cells;

  public WritableRow(final String rowKey) {
    this.rowKey = rowKey;
    this.cells = new ArrayList<>();
  }

  public void addCell(final WritableCells cell) {
    this.cells.add(cell);
  }

  public String rowKey() {
    return this.rowKey;
  }

  public List<WritableCells> cells() {
    return this.cells;
  }

  @Override
  public String toString() {
    return "WritableRow [rowKey=" + this.rowKey + ", cells=" + this.cells + "]";
  }
}
