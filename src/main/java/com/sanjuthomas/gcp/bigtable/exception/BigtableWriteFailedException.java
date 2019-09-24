package com.sanjuthomas.gcp.bigtable.exception;

/**
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
public class BigtableWriteFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public BigtableWriteFailedException(final String message) {
    super(message);
  }
}
