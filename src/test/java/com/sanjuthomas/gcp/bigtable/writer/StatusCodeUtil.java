package com.sanjuthomas.gcp.bigtable.writer;

import com.google.api.gax.rpc.StatusCode;

/**
 * 
 * @author Sanju Thomas
 *
 */
public interface StatusCodeUtil {
  
  public static final StatusCode LOCAL_STATUS = new StatusCode() {
      @Override
      public Code getCode() {
        return Code.INTERNAL;
      }
  
      @Override
      public Object getTransportCode() {
        return null;
      }
    };
}

