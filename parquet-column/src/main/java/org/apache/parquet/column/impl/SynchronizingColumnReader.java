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
package org.apache.parquet.column.impl;

import java.util.PrimitiveIterator;

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * A {@link ColumnReader} implementation for utilizing indexes. When filtering
 * using column indexes we might skip reading some pages for different columns.
 * Because the rows are not aligned between the pages of the different columns
 * it might be required to skip some values in this {@link ColumnReader} so we
 * provide only the required values for the higher API ({@link RecordReader})
 * and they do not need to handle or know about the skipped pages. The values
 * (and the related rl and dl) are skipped based on the iterator of the required
 * row indexes and the first row index of each page.<br>
 * For example:
 *
 * <pre>
 * rows   col1   col2   col3
 *      ┌──────┬──────┬──────┐
 *   0  │  p0  │      │      │
 *      ╞══════╡  p0  │  p0  │
 *  20  │ p1(X)│------│------│
 *      ╞══════╪══════╡      │
 *  40  │ p2(X)│      │------│
 *      ╞══════╡ p1(X)╞══════╡
 *  60  │ p3(X)│      │------│
 *      ╞══════╪══════╡      │
 *  80  │  p4  │      │  p1  │
 *      ╞══════╡  p2  │      │
 * 100  │  p5  │      │      │
 *      └──────┴──────┴──────┘
 * </pre>
 *
 * The pages 1, 2, 3 in col1 are skipped so we have to skip the rows [20, 79].
 * Because page 1 in col2 contains values only for the rows [40, 79] we skip
 * this entire page as well. To synchronize the row reading we have to skip the
 * values (and the related rl and dl) for the rows [20, 39] in the end of the
 * page 0 for col2. Similarly, we have to skip values while reading page0 and
 * page1 for col3.
 */
class SynchronizingColumnReader extends ColumnReaderBase {

  private final PrimitiveIterator.OfLong rowIndexes;
  private long currentRow;
  private long targetRow;
  private long lastRowInPage;
  private int valuesReadFromPage;

  SynchronizingColumnReader(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter,
      ParsedVersion writerVersion, PrimitiveIterator.OfLong rowIndexes) {
    super(path, pageReader, converter, writerVersion);
    this.rowIndexes = rowIndexes;
    targetRow = Long.MIN_VALUE;
    consume();
  }

  @Override
  boolean isPageFullyConsumed() {
    return getPageValueCount() <= valuesReadFromPage || lastRowInPage < targetRow;
  }

  @Override
  boolean isFullyConsumed() {
    return !rowIndexes.hasNext();
  }

  @Override
  boolean skipRL(int rl) {
    ++valuesReadFromPage;
    if (rl == 0) {
      ++currentRow;
      if (currentRow > targetRow) {
        targetRow = rowIndexes.hasNext() ? rowIndexes.nextLong() : Long.MAX_VALUE;
      }
    }
    return currentRow < targetRow;
  }

  @Override
  protected void newPageInitialized(DataPage page) {
    long firstRowIndex = page.getFirstRowIndex()
        .orElseThrow(() -> new IllegalArgumentException("Missing firstRowIndex for synchronizing values"));
    int rowCount = page.getIndexRowCount()
        .orElseThrow(() -> new IllegalArgumentException("Missing rowCount for synchronizing values"));
    currentRow = firstRowIndex - 1;
    lastRowInPage = firstRowIndex + rowCount - 1;
    valuesReadFromPage = 0;
  }

}
