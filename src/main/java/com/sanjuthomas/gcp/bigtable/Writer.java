/*
 * Copyright (c) 2019 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *
 * A writer that can buffer the transformed messages and flush to the storage.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public interface Writer<T, R> {

  /**
   * Flush out the messages from buffer to the store.
   */
  void flush();

  /**
   * Add the given message into local buffer and return the size of the buffer.
   *
   * @param t
   */
  int buffer(T t);

  /**
   * Close any resources open.
   */
  void close();

  /**
   * Return the current size of the buffer.
   * @return
   */
  int bufferSize();

}
