/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.primitive;

import java.io.IOException;

import parquet.io.Binary;

/**
 * base class to implement an encoding for a given column
 *
 * pages are homogeneous (store a single type)
 * Usually only one of the read*() methods is overridden
 *
 * @author Julien Le Dem
 *
 */
public abstract class PrimitiveColumnReader {

  /**
   * Called to initialize the column reader with a new page.
   *
   * The underlying implementation knows how much data to read
   * <ul>The page contains the bytes for:
   *  <li> repetition levels column
   *  <li> definition levels column
   *  <li> data column
   * </ul>
   * Each column reader knows how much data to read and returns the next offset for the next column
   * The data column always reads to the end and returns the array size.
   * @param valueCount count of values in this page
   * @param page the array to read from containing the page data (repetition levels, definition levels, data)
   * @param offset where to start reading from in the page
   * @return the offset to read from the next column (the page length in the case of the data column which is last in the page)
   * @throws IOException
   */
  public abstract int initFromPage(long valueCount, byte[] page, int offset) throws IOException;

  /**
   * @return the next boolean from the page
   */
  public boolean readBoolean() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public int readByte() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public float readFloat() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public Binary readBytes() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public double readDouble() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public int readInteger() {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the next boolean from the page
   */
  public long readLong() {
    throw new UnsupportedOperationException();
  }
}