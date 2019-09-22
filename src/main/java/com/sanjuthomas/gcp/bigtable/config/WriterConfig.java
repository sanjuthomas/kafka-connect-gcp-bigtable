package com.sanjuthomas.gcp.bigtable.config;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Sanju Thomas
 *
 */
public class WriterConfig {

  private final String table;
  private final String keyFile;
  private final String project;
  private final String instance;
  private ErrorHandlerConfig errorHandlerConfig;

  public WriterConfig(final String keyFile, final String project, final String instance,
      final String table) {
    this.keyFile = keyFile;
    this.project = project;
    this.instance = instance;
    this.table = table;
  }

  public String table() {
    return this.table;
  }

  public String keyFile() {
    return this.keyFile;
  }

  public String project() {
    return this.project;
  }

  public String instance() {
    return this.instance;
  }

  public ErrorHandlerConfig getErrorHandlerConfig() {
    if (null == errorHandlerConfig) {
      return new ErrorHandlerConfig(3, 3, true,
          Arrays.asList("com.google.api.gax.rpc.ApiException"));
    }
    return errorHandlerConfig;
  }

  public WriterConfig setErrorHandlerConfig(ErrorHandlerConfig errorHandlerConfig) {
    this.errorHandlerConfig = errorHandlerConfig;
    return this;
  }

  public static class ErrorHandlerConfig {

    public ErrorHandlerConfig(final int maxRetryCount, final int retryBackOffSeconds,
        final boolean exponentialBackOff, final List<String> retryableExceptions) {
      this.maxRetryCount = maxRetryCount;
      this.retryBackOffSeconds = retryBackOffSeconds;
      this.exponentialBackOff = exponentialBackOff;
      this.retryableExceptions = retryableExceptions;
    }

    private int maxRetryCount;
    private int retryBackOffSeconds;
    private boolean exponentialBackOff;
    private List<String> retryableExceptions;

    public int maxRetryCount() {
      return maxRetryCount;
    }

    public int retryBackOffSeconds() {
      return retryBackOffSeconds;
    }

    public boolean exponentialBackOff() {
      return exponentialBackOff;
    }

    public List<String> retryableExceptions() {
      return retryableExceptions;
    }
  }
}
