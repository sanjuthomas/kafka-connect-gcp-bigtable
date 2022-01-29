package com.sanjuthomas.gcp.bigtable.exception;

/**
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
public class BigtableSinkInitializationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public BigtableSinkInitializationException(final String message, final Exception e) {
    super(message, e);
  }
}