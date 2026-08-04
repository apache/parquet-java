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
package org.apache.parquet.internal.filter2.columnindex;

import java.util.PrimitiveIterator;
import org.apache.parquet.filter2.columnindex.RowRanges.Builder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

/**
 * @deprecated moved to {@link org.apache.parquet.filter2.columnindex.RowRanges}. This type is
 *     retained only so that the released
 *     {@code ParquetFileReader#readFilteredRowGroup(int, RowRanges)} signature keeps linking; it
 *     will be removed in 2.0. Use {@link org.apache.parquet.filter2.columnindex.RowRanges} instead.
 */
@Deprecated
public class RowRanges extends org.apache.parquet.filter2.columnindex.RowRanges {

  /**
   * Creates a {@link org.apache.parquet.filter2.columnindex.RowRanges} covering the given pages. The row bounds of
   * each page are read from the offset index (see {@code OffsetIndex#getFirstRowIndex} and
   * {@code OffsetIndex#getLastRowIndex}); adjacent page ranges are coalesced.
   *
   * @param rowCount row count of the row-group
   * @param pageIndexes ascending iterator of the selected page indexes
   * @param offsetIndex the offset index providing the row bounds of each page
   * @return the row ranges covering the selected pages
   * @deprecated use {@link org.apache.parquet.filter2.columnindex.RowRanges#builder()} together with
   *     {@link org.apache.parquet.filter2.columnindex.RowRanges.Builder#addSelectedRange(long, long)}
   */
  @Deprecated
  public static org.apache.parquet.filter2.columnindex.RowRanges create(
      long rowCount, PrimitiveIterator.OfInt pageIndexes, OffsetIndex offsetIndex) {
    Builder builder = builder();
    while (pageIndexes.hasNext()) {
      int pageIndex = pageIndexes.nextInt();
      builder.addSelectedRange(
          offsetIndex.getFirstRowIndex(pageIndex), offsetIndex.getLastRowIndex(pageIndex, rowCount));
    }
    return builder.build();
  }
}
