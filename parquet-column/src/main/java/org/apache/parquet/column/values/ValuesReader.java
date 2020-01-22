/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

/**
 * Base class to implement an encoding for a given column type.
 *
 * A ValuesReader is provided with a page (byte-buffer) and is responsible for
 * deserializing the primitive values stored in that page.
 *
 * Given that pages are homogeneous (store only a single type), typical
 * subclasses will only override one of the read*() methods.
 */
public abstract class ValuesReader {

  // To be used to maintain the deprecated behavior of getNextOffset(); -1 means
  // undefined
  private int actualOffset = -1;
  private int nextOffset;

  /**
   * Called to initialize the column reader from a part of a page.
   *
   * The underlying implementation knows how much data to read, so a length is not
   * provided.
   *
   * Each page may contain several sections:
   * <ul>
   * <li>repetition levels column
   * <li>definition levels column
   * <li>data column
   * </ul>
   *
   * This function is called with 'offset' pointing to the beginning of one of
   * these sections, and should return the offset to the section following it.
   *
   * @param valueCount count of values in this page
   * @param page the array to read from containing the page data (repetition
   * levels, definition levels, data)
   * @param offset where to start reading from in the page
   *
   * @throws IOException
   * @deprecated Will be removed in 2.0.0
   */
  @Deprecated
  public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
    if (offset < 0) {
      throw new IllegalArgumentException("Illegal offset: " + offset);
    }
    actualOffset = offset;
    ByteBuffer pageWithOffset = page.duplicate();
    pageWithOffset.position(offset);
    initFromPage(valueCount, ByteBufferInputStream.wrap(pageWithOffset));
    actualOffset = -1;
  }

  /**
   * Same functionality as method of the same name that takes a ByteBuffer instead
   * of a byte[].
   *
   * This method is only provided for backward compatibility and will be removed
   * in a future release. Please update any code using it as soon as possible.
   * 
   * @see #initFromPage(int, ByteBuffer, int)
   */
  @Deprecated
  public void initFromPage(int valueCount, byte[] page, int offset) throws IOException {
    this.initFromPage(valueCount, ByteBuffer.wrap(page), offset);
  }

  /**
   * Called to initialize the column reader from a part of a page.
   *
   * Implementations must consume all bytes from the input stream, leaving the
   * stream ready to read the next section of data. The underlying implementation
   * knows how much data to read, so a length is not provided.
   *
   * Each page may contain several sections:
   * <ul>
   * <li>repetition levels column
   * <li>definition levels column
   * <li>data column
   * </ul>
   *
   * @param valueCount count of values in this page
   * @param in an input stream containing the page data at the correct offset
   *
   * @throws IOException if there is an exception while reading from the input
   * stream
   */
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    if (actualOffset != -1) {
      throw new UnsupportedOperationException(
          "Either initFromPage(int, ByteBuffer, int) or initFromPage(int, ByteBufferInputStream) must be implemented in "
              + getClass().getName());
    }
    initFromPage(valueCount, in.slice(valueCount), 0);
  }

  /**
   * Called to return offset of the next section
   * 
   * @return offset of the next section
   * @deprecated Will be removed in 2.0.0
   */
  @Deprecated
  public int getNextOffset() {
    if (nextOffset == -1) {
      throw new ParquetDecodingException("Unsupported: cannot get offset of the next section.");
    } else {
      return nextOffset;
    }
  }

  // To be used to maintain the deprecated behavior of getNextOffset();
  // bytesRead is the number of bytes read in the last initFromPage call
  protected void updateNextOffset(int bytesRead) {
    nextOffset = actualOffset == -1 ? -1 : actualOffset + bytesRead;
  }

  /**
   * usable when the encoding is dictionary based
   * 
   * @return the id of the next value from the page
   */
  public int readValueDictionaryId() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public boolean readBoolean() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next Binary from the page
   */
  public Binary readBytes() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next float from the page
   */
  public float readFloat() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next double from the page
   */
  public double readDouble() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next integer from the page
   */
  public int readInteger() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next long from the page
   */
  public long readLong() {
    throw new UnsupportedOperationException();
  }

  /**
   * Skips the next value in the page
   */
  abstract public void skip();

  /**
   * Skips the next n values in the page
   *
   * @param n the number of values to be skipped
   */
  public void skip(int n) {
    for (int i = 0; i < n; ++i) {
      skip();
    }
  }
}
