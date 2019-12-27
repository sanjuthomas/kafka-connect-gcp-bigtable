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

package com.sanjuthomas.gcp.bigtable.writer;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import com.google.api.gax.rpc.ApiException;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;

/**
 *
 * @author Sanju Thomas
 * @since 1.0.3
 *
 */
@Evolving
public class ErrorHandler {

  private ErrorHandlerConfig config;
  private AtomicInteger counter = new AtomicInteger(0);

  public ErrorHandler(final ErrorHandlerConfig config) {
    this.config = config;
  }

  public Result handle(Throwable exception) {
    if (exception instanceof ApiException && ((ApiException) exception).isRetryable()
        && counter.incrementAndGet() <= config.maxRetryCount()) {
      return new Result(true, retryBackOffSeconds(), counter.get());
    }
    return new Result(false, 0, counter.get());
  }

  private long retryBackOffSeconds() {
    if (config.exponentialBackoff()) {
      return counter.longValue() * config.retryBackoffSeconds();
    }
    return config.retryBackoffSeconds();
  }

  public void reset() {
    counter.set(0);
  }

  class Result {

    private boolean retry;
    private long secondsToSleep;
    private int attempt;

    Result(final boolean retry, final long secondsToSleep, final int attempt) {
      this.retry = retry;
      this.secondsToSleep = secondsToSleep;
      this.attempt = attempt;
    }

    public boolean retry() {
      return retry;
    }

    public long secondsToSleep() {
      return secondsToSleep;
    }

    public int attempt() {
      return attempt;
    }
  }
}
