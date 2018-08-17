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
 * one data page in a chunk
 */
abstract public class DataPage extends Page {

  private final int valueCount;
  private final long firstRowIndex;
  private final int rowCount;

  DataPage(int compressedSize, int uncompressedSize, int valueCount) {
    this(compressedSize, uncompressedSize, valueCount, -1, -1);
  }

  DataPage(int compressedSize, int uncompressedSize, int valueCount, long firstRowIndex, int rowCount) {
    super(compressedSize, uncompressedSize);
    this.valueCount = valueCount;
    this.firstRowIndex = firstRowIndex;
    this.rowCount = rowCount;
  }

  /**
   * @return the number of values in that page
   */
  public int getValueCount() {
    return valueCount;
  }

  /**
   * @return the index of the first row in this page
   * @throws NotInPageFilteringModeException
   *           if page filtering mode is not active
   * @see PageReadStore#isInPageFilteringMode()
   */
  public long getFirstRowIndex() {
    if (firstRowIndex < 0) {
      throw new NotInPageFilteringModeException("First row index is not available");
    }
    return firstRowIndex;
  }

  /**
   * @return the number of rows in this page
   * @throws NotInPageFilteringModeException
   *           if page filtering mode is not active; thrown only in case of {@link DataPageV1}
   * @see PageReadStore#isInPageFilteringMode()
   */
  public int getRowCount() {
    if (rowCount < 0) {
      throw new NotInPageFilteringModeException(
          "Row count is not available");
    }
    return rowCount;
  }

  public abstract <T> T accept(Visitor<T> visitor);

  public static interface Visitor<T> {

    T visit(DataPageV1 dataPageV1);

    T visit(DataPageV2 dataPageV2);

  }

}
