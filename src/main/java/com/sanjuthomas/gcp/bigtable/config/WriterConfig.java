/*
 * Copyright (c) 2019 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.config;

import org.apache.kafka.common.annotation.InterfaceStability.Stable;

/**
 *
 * In memory representation of Writer configuration.
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Stable
public class WriterConfig {

  private final String table;
  private final String keyFile;
  private final String project;
  private final String instance;
  private ErrorHandlerConfig errorHandlerConfig;
  private Integer bulkMutateRowsMaxSize;
  private Boolean continueAfterWriteError;

  public WriterConfig(final String keyFile, final String project, final String instance,
      final String table, final Integer bulkMutateRowsMaxSize, final Boolean continueAfterWriteError) {
    this.keyFile = keyFile;
    this.project = project;
    this.instance = instance;
    this.table = table;
    this.bulkMutateRowsMaxSize = bulkMutateRowsMaxSize;
    this.continueAfterWriteError = continueAfterWriteError;
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

  public int bulkMutateRowsMaxSize() {
    return bulkMutateRowsMaxSize;
  }

  public boolean continueAfterWriteError() {
    return continueAfterWriteError;
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
