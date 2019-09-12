package com.sanjuthoas.gcp.bigtable;

/**
 *
 * @author Sanju Thomas
 *
 */
@FunctionalInterface
public interface Result<T> {

  public T get();

}
