package com.sanjuthoas.gcp.bigtable.exception;

/**
 *
 * @author Sanju Thomas
 *
 */
public class RowKeyNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RowKeyNotFoundException(final String message) {
    super(message);
  }
}
