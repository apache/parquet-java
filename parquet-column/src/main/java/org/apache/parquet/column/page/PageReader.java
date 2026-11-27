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
   * @return the total number of values this reader will yield - which may be a filtered subset of
   * the column chunk's values, e.g. when row-range/column-index filtering is applied
   */
  long getTotalValueCount();

  /**
   * @return the sum of the uncompressed sizes of the (data) pages this reader will yield - which may
   * be a filtered subset of the column chunk's pages, e.g. when row-range/column-index filtering is
   * applied - or exactly -1 if unknown. A return value of 0 is a legitimate, meaningful result (for
   * example an empty column chunk), distinct from "unknown"; callers that use this to pre-size a
   * buffer must treat only -1 as "no estimate available" and fall back to their default behavior.
   * This value is derived from unvalidated page-header metadata in the Parquet file, not verified
   * against the actual bytes read; callers that pre-size an allocation from it should bound or chunk
   * that allocation (e.g. as {@code org.apache.parquet.ParquetReadOptions#getMaxAllocationSize()}
   * does internally) rather than allocating the full reported size in one call.
   */
  default long getTotalUncompressedSize() {
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
