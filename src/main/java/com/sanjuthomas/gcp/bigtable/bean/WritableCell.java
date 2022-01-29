package com.sanjuthomas.gcp.bigtable.bean;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;
import com.google.protobuf.ByteString;

/**
 * 
 * Represent a Cell in the Bigtable.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
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
}