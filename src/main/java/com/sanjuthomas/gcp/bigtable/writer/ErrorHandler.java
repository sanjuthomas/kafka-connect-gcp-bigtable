package com.sanjuthomas.gcp.bigtable.writer;

import java.util.concurrent.atomic.AtomicInteger;
import com.google.api.gax.rpc.ApiException;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ErrorHandler {

  private ErrorHandlerConfig config;
  private AtomicInteger counter = new AtomicInteger(0);

  public ErrorHandler(final ErrorHandlerConfig config) {
    this.config = config;
  }

  public Result handle(Throwable exception) {
    if (exception instanceof ApiException) {
      if (((ApiException) exception).isRetryable()) {
        if (counter.incrementAndGet() <= config.maxRetryCount()) {
          return new Result(true, retryBackOffSeconds());
        }
      }
    }
    return new Result(false, 0);
  }

  private long retryBackOffSeconds() {
    if (config.exponentialBackOff()) {
      return counter.longValue() * config.retryBackOffSeconds();
    }
    return config.retryBackOffSeconds();
  }

  class Result {

    private boolean retry;
    private long secondsToSleep;

    Result(boolean retry, long secondsToSleep) {
      this.retry = retry;
      this.secondsToSleep = secondsToSleep;
    }

    public boolean retry() {
      return retry;
    }

    public long secondsToSleep() {
      return secondsToSleep;
    }
  }
}
