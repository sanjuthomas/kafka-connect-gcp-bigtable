/*
 *
 *  Copyright (c) 2023 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  you may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package com.sanjuthomas.gcp.bigtable.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.api.gax.rpc.ApiException;
import com.sanjuthomas.gcp.bigtable.config.WriterConfig.ErrorHandlerConfig;
import com.sanjuthomas.gcp.bigtable.writer.ErrorHandler.Result;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class ErrorHandlerTest {

  private ErrorHandler handlerExponentialBackoff;
  private ErrorHandler handlerSingleBackoff;
  private ErrorHandler handler;

  @BeforeEach
  public void setUp() {
    handlerExponentialBackoff = new ErrorHandler(new ErrorHandlerConfig(3, 3, true));
    handlerSingleBackoff = new ErrorHandler(new ErrorHandlerConfig(1, 3, true));
    handler = new ErrorHandler(new ErrorHandlerConfig(3, 3, false));
  }

  @Test
  public void shouldHandleExponentialBackOff() {
    final ApiException exception = new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, true);
    Result result = handlerExponentialBackoff.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.secondsToSleep());
    assertEquals(1, result.attempt());
    result = handlerExponentialBackoff.handle(exception);
    assertTrue(result.retry());
    assertEquals(2, result.attempt());
    assertEquals(6, result.secondsToSleep());
    result = handlerExponentialBackoff.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.attempt());
    assertEquals(9, result.secondsToSleep());
    result = handlerExponentialBackoff.handle(exception);
    assertFalse(result.retry());
    assertEquals(4, result.attempt());
    assertEquals(0, result.secondsToSleep());
    handlerExponentialBackoff.reset();
  }
  
  @Test
  public void shouldHandle() {
    final ApiException exception = new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, true);
    Result result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(1, result.attempt());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(2, result.attempt());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertTrue(result.retry());
    assertEquals(3, result.attempt());
    assertEquals(3, result.secondsToSleep());
    result = handler.handle(exception);
    assertFalse(result.retry());
    assertEquals(4, result.attempt());
    assertEquals(0, result.secondsToSleep());
  }
  
  @Test
  public void shouldRestHandler() {
    final ApiException exception = new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, true);
    Result result = handlerSingleBackoff.handle(exception);
    assertTrue(result.retry());
    assertEquals(1, result.attempt());
    assertEquals(3, result.secondsToSleep());
    result = handlerSingleBackoff.handle(exception);
    assertFalse(result.retry());
    handlerSingleBackoff.reset();
    result = handlerSingleBackoff.handle(exception);
    assertTrue(result.retry());
    assertEquals(1, result.attempt());
    assertEquals(3, result.secondsToSleep());
    result = handlerSingleBackoff.handle(exception);
    assertFalse(result.retry());
  }
  
  @Test
  public void shouldNotRetry() {
    final ApiException exception = new ApiException(new Exception(), StatusCodeUtil.LOCAL_STATUS, false);
    Result result = handler.handle(exception);
    assertFalse(result.retry());
    
    final Exception ex = new Exception();
    result = handler.handle(ex);
    assertFalse(result.retry());
    assertEquals(0, result.attempt());
  }

}
