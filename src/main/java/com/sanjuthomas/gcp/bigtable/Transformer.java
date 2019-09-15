package com.sanjuthomas.gcp.bigtable;

/**
 *
 * Transform a given type to another type.
 *
 * @author Sanju Thomas
 *
 */
@FunctionalInterface
public interface Transformer<T, R> {

  /**
   * Convert the given type to another.
   * 
   * @param t
   * @return
   */
  R transform(T t);

}
