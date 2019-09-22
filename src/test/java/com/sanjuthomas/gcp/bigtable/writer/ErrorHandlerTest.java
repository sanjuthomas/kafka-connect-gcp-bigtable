package com.sanjuthomas.gcp.bigtable.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;
import com.sanjuthomas.gcp.bigtable.writer.ErrorHandler.Result;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ErrorHandlerTest {

  private ErrorHandler handlerExponentialBackOff;
  private ErrorHandler handler;

  private static final StatusCode LOCAL_STATUS = new StatusCode() {
    @Override
    public Code getCode() {
      return Code.INTERNAL;
    }

    @Override
    public Object getTransportCode() {
      return null;
    }
  };

  @BeforeEach
  public void setUp() {
    handlerExponentialBackOff = new ErrorHandler(new ErrorHandlerConfig(3, 3, true));
    handler = new ErrorHandler(new ErrorHandlerConfig(3, 3, false));
  }

  @Test
  public void shouldHandleExponentialBackOff() {
    final ApiException exception = new ApiException(new Exception(), LOCAL_STATUS, true);
    Result result = handlerExponentialBackOff.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.secondsToSleep());
    result = handlerExponentialBackOff.handle(exception);
    assertTrue(result.retry());
    assertEquals(6, result.secondsToSleep());
    result = handlerExponentialBackOff.handle(exception);
    assertTrue(result.retry());
    assertEquals(9, result.secondsToSleep());
    result = handlerExponentialBackOff.handle(exception);
    assertFalse(result.retry());
    assertEquals(0, result.secondsToSleep());
  }
  
  @Test
  public void shouldHandle() {
    final ApiException exception = new ApiException(new Exception(), LOCAL_STATUS, true);
    Result result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertFalse(result.retry());
    assertEquals(0, result.secondsToSleep());
  }
  
  @Test
  public void shouldNotRetry() {
    final ApiException exception = new ApiException(new Exception(), LOCAL_STATUS, false);
    Result result = handler.handle(exception);
    assertFalse(result.retry());
    
    final Exception ex = new Exception();
    result = handler.handle(ex);
    assertFalse(result.retry());
  }

}
