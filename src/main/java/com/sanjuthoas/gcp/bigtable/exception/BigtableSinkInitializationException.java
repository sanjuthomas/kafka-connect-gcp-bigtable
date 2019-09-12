package com.sanjuthoas.gcp.bigtable.exception;

public class BigtableSinkInitializationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public BigtableSinkInitializationException(final String message, final Exception e) {
    super(message, e);
  }
}
