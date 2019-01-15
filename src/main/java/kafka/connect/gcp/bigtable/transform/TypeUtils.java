package kafka.connect.gcp.bigtable.transform;

import java.nio.ByteBuffer;
import com.google.protobuf.ByteString;

/**
 *
 * @author Sanju Thomas
 *
 */
public class TypeUtils {

  public static ByteString toByteString(final Object value) {
    if (value instanceof ByteBuffer) {
      return ByteString.copyFrom((ByteBuffer) value);
    }
    if (value instanceof byte[]) {
      return ByteString.copyFrom((byte[]) value);
    }
    return ByteString.copyFromUtf8(value.toString());
  }

}
