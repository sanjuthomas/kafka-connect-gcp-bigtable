package kafka.connect.gcp.bigtable.bean;

import com.google.protobuf.ByteString;

/**
 *
 * @author Sanju Thomas
 *
 */
public class WritableCell {

  private final ByteString qualifier;
  private final ByteString value;

  public WritableCell(final ByteString qualifier, final ByteString value) {
    this.qualifier = qualifier;
    this.value = value;
  }

  public ByteString qualifier() {
    return this.qualifier;
  }

  public ByteString value() {
    return this.value;
  }

  @Override
  public String toString() {
    return "WritableCell [qualifier=" + this.qualifier.toStringUtf8() + ", value="
        + this.value.toStringUtf8() + "]";
  }

}
