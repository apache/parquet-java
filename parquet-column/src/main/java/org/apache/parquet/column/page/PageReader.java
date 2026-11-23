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
package org.apache.parquet.column.page;

/**
 * Reader for a sequence a page from a given column chunk
 */
public interface PageReader extends AutoCloseable {

  /**
   * @return the dictionary page in that chunk or null if none
   */
  DictionaryPage readDictionaryPage();

  /**
   * @return the total number of values in the column chunk
   */
  long getTotalValueCount();

  /**
   * @return the sum of the uncompressed sizes of the (data) pages in this column chunk, or -1 if
   * unknown. Callers that use this to pre-size a buffer must treat -1 (or any non-positive value)
   * as "no estimate available" and fall back to their default behavior.
   */
  default long getUncompressedSize() {
    return -1;
  }

  /**
   * @return the next page in that chunk or null if after the last page
   */
  DataPage readPage();

  /**
   * Releases any resources this reader holds (buffers, native handles, etc.).
   */
  @Override
  default void close() {}
}
