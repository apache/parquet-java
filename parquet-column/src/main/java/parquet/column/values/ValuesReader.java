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
package parquet.column.values;

import java.io.IOException;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

/**
 * Base class to implement an encoding for a given column type.
 *
 * A ValuesReader is provided with a page (byte-array) and is responsible
 * for deserializing the primitive values stored in that page.
 *
 * Given that pages are homogeneous (store only a single type), typical subclasses
 * will only override one of the read*() methods.
 *
 * @author Julien Le Dem
 */
public abstract class ValuesReader {

  /**
   * Called to initialize the column reader from a part of a page.
   *
   * The underlying implementation knows how much data to read, so a length
   * is not provided.
   *
   * Each page may contain several sections:
   * <ul>
   *  <li> repetition levels column
   *  <li> definition levels column
   *  <li> data column
   * </ul>
   *
   * This function is called with 'offset' pointing to the beginning of one of these sections,
   * and should return the offset to the section following it.
   *
   * @param valueCount count of values in this page
   * @param page the array to read from containing the page data (repetition levels, definition levels, data)
   * @param offset where to start reading from in the page
   *
   * @throws IOException
   */
  public abstract void initFromPage(int valueCount, byte[] page, int offset) throws IOException;
  
  /**
   * Called to return offset of the next section
   * @return offset of the next section
   */
  public int getNextOffset() {
    throw new ParquetDecodingException("Unsupported: cannot get offset of the next section.");
  }

  /**
   * usable when the encoding is dictionary based
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
}

