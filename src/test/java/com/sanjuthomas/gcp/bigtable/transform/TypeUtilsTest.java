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
