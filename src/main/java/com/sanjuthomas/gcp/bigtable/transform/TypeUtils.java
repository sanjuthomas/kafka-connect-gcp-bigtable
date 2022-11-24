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
