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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

/**
 * Base class to implement an encoding for a given column type.
 *
 * A ValuesReader is provided with a page (byte-buffer) and is responsible
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
   * Implementations must consume all bytes from the input stream, leaving the
   * stream ready to read the next section of data. The underlying
   * implementation knows how much data to read, so a length is not provided.
   *
   * Each page may contain several sections:
   * <ul>
   *  <li> repetition levels column
   *  <li> definition levels column
   *  <li> data column
   * </ul>
   *
   * @param valueCount count of values in this page
   * @param in an input stream containing the page data at the correct offset
   *
   * @throws IOException
   */
  public abstract void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException;

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

