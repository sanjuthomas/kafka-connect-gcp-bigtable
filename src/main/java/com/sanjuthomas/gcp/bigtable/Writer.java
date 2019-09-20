package com.sanjuthomas.gcp.bigtable;

/**
 * A writer that can buffer the transformed messages and flush to the storage.
 *
 * @author Sanju Thomas
 *
 */
public interface Writer<T, R> {

  /**
   * Flush out the messages from buffer to the store.
   */
  void flush();

  /**
   * Add the given message into local buffer and return the size of the buffer.
   *
   * @param t
   */
  int buffer(T t);

  /**
   * Close any resources open.
   */
  void close();
  
  /**
   * Return the current size of the buffer.
   * @return
   */
  int bufferSize();

}
