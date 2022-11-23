package com.sanjuthomas.gcp.bigtable.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import com.google.protobuf.ByteString;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class TypeUtilsTest {

  @Test
  public void shouldConvertToByteString() {
    final ByteBuffer buffer = ByteBuffer.wrap("data".getBytes());
    assertEquals(ByteString.copyFrom("data".getBytes()), TypeUtils.toByteString(buffer));
    assertEquals(ByteString.copyFrom("data".getBytes()),
        TypeUtils.toByteString("data".getBytes()));
    assertEquals(ByteString.copyFrom("data".getBytes()), TypeUtils.toByteString("data"));
  }

  @Test
  public void shouldHandleNullValue(){
    assertEquals("", TypeUtils.toByteString(null).toStringUtf8());
  }

}
