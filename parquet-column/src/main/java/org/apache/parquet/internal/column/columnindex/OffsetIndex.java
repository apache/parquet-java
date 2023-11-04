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
package org.apache.parquet.internal.column.columnindex;

import java.util.Optional;

/**
 * Offset index containing the offset and size of the page and the index of the first row in the page.
 *
 * @see org.apache.parquet.format.OffsetIndex
 */
public interface OffsetIndex {
  /**
   * @return the number of pages
   */
  public int getPageCount();

  /**
   * @param pageIndex
   *          the index of the page
   * @return the offset of the page in the file
   */
  public long getOffset(int pageIndex);

  /**
   * @param pageIndex
   *          the index of the page
   * @return the compressed size of the page (including page header)
   */
  public int getCompressedPageSize(int pageIndex);

  /**
   * @param pageIndex
   *          the index of the page
   * @return the index of the first row in the page
   */
  public long getFirstRowIndex(int pageIndex);

  /**
   * @param pageIndex
   *         the index of the page
   * @return the original ordinal of the page in the column chunk
   */
  public default int getPageOrdinal(int pageIndex) {
    return pageIndex;
  }

  /**
   * @param pageIndex
   *          the index of the page
   * @param rowGroupRowCount
   *          the total number of rows in the row-group
   * @return the calculated index of the last row of the given page
   */
  public default long getLastRowIndex(int pageIndex, long rowGroupRowCount) {
    int nextPageIndex = pageIndex + 1;
    return (nextPageIndex >= getPageCount() ? rowGroupRowCount : getFirstRowIndex(nextPageIndex)) - 1;
  }

  /**
   * @param pageIndex
   *          the index of the page
   * @return unencoded/uncompressed size for BYTE_ARRAY types; or empty for other types.
   */
  public Optional<Long> getUnencodedByteArrayDataBytes(int pageIndex);
}
