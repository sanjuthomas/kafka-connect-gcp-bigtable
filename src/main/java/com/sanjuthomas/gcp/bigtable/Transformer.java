package com.sanjuthomas.gcp.bigtable;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *
 * Transform a given type to another type.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
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
