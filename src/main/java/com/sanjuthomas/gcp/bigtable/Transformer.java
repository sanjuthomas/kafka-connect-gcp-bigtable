package com.sanjuthomas.gcp.bigtable;

/**
 *
 * @author Sanju Thomas
 *
 */
@FunctionalInterface
public interface Transformer<T, R> {

  R transform(T t);

}
