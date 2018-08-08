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
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * A {@link ColumnReader} implementation that synchronize the values for skipped pages.
 * 
 * @see PageReadStore#isRowSynchronizationRequired()
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
  boolean skipLevels(int rl, int dl) {
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
    long firstRowIndex = page.getFirstRowIndex();
    currentRow = firstRowIndex - 1;
    lastRowInPage = firstRowIndex + page.getRowCount() - 1;
    valuesReadFromPage = 0;
  }

}
