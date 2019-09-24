package com.sanjuthomas.gcp.bigtable;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@FunctionalInterface
@Stable
public interface Parser<T, R> {

  public R parse(T t);

}
