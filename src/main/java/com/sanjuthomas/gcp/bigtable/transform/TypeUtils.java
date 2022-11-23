package com.sanjuthomas.gcp.bigtable.transform;

import java.nio.ByteBuffer;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import com.google.protobuf.ByteString;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Evolving
public class TypeUtils {

  /**
   * Convert the given object to ByteString.
   * 
   * @param value
   * @return ByteString
   */
  public static ByteString toByteString(final Object value) {
    if(null == value) {
      return ByteString.EMPTY;
    }
    if (value instanceof ByteBuffer) {
      return ByteString.copyFrom((ByteBuffer) value);
    }
    if (value instanceof byte[]) {
      return ByteString.copyFrom((byte[]) value);
    }
    return ByteString.copyFromUtf8(value.toString());
  }
}
