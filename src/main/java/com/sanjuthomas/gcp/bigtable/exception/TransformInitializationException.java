package com.sanjuthomas.gcp.bigtable.exception;

/**
 * 
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
public class TransformInitializationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public TransformInitializationException(final String message) {
    super(message);
  }
}
