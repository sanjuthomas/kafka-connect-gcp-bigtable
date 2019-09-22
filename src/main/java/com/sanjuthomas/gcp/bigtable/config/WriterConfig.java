package com.sanjuthomas.gcp.bigtable.config;

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
    return errorHandlerConfig;
  }

  public WriterConfig setErrorHandlerConfig(ErrorHandlerConfig errorHandlerConfig) {
    this.errorHandlerConfig = errorHandlerConfig;
    return this;
  }

  public static class ErrorHandlerConfig {

    private int maxRetryCount;
    private int retryBackoffSeconds;
    private boolean exponentialBackoff;
    
    public ErrorHandlerConfig(final int maxRetryCount, final int retryBackoffSeconds,
        final boolean exponentialBackoff) {
      this.maxRetryCount = maxRetryCount;
      this.retryBackoffSeconds = retryBackoffSeconds;
      this.exponentialBackoff = exponentialBackoff;
    }

    public int maxRetryCount() {
      return maxRetryCount;
    }

    public int retryBackoffSeconds() {
      return retryBackoffSeconds;
    }

    public boolean exponentialBackoff() {
      return exponentialBackoff;
    }
  }
}
