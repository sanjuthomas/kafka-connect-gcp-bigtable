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

package com.sanjuthomas.gcp.bigtable;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 * Transform a given type to another type.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 */
@FunctionalInterface
@Stable
public interface Transformer<T, R> {

  /**
   * Convert the given type to another.
   *
   * @param t
   * @return
   */
  R transform(T t);

}
