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
package parquet.column.impl;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.unmodifiableMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriteStore;
import parquet.column.ColumnWriter;
import parquet.column.ParquetProperties;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;
import parquet.schema.MessageType;

public class ColumnWriteStoreV2 implements ColumnWriteStore {

  // will wait for at least that many records before checking again
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;
  // will flush even if size bellow the threshold by this much to facilitate page alignment
  private static final float THRESHOLD_TOLERANCE_RATIO = 0.1f; // 10 %

  private final Map<ColumnDescriptor, ColumnWriterV2> columns;
  private final Collection<ColumnWriterV2> writers;
  private long rowCount;
  private long rowCountForNextSizeCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
  private final long thresholdTolerance;

  private int pageSizeThreshold;

  public ColumnWriteStoreV2(
      MessageType schema,
      PageWriteStore pageWriteStore,
      int pageSizeThreshold,
      ParquetProperties parquetProps) {
    super();
    this.pageSizeThreshold = pageSizeThreshold;
    this.thresholdTolerance = (long)(pageSizeThreshold * THRESHOLD_TOLERANCE_RATIO);
    Map<ColumnDescriptor, ColumnWriterV2> mcolumns = new TreeMap<ColumnDescriptor, ColumnWriterV2>();
    for (ColumnDescriptor path : schema.getColumns()) {
      PageWriter pageWriter = pageWriteStore.getPageWriter(path);
      mcolumns.put(path, new ColumnWriterV2(path, pageWriter, parquetProps, pageSizeThreshold));
    }
    this.columns = unmodifiableMap(mcolumns);
    this.writers = this.columns.values();
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    return columns.get(path);
  }

  public Set<ColumnDescriptor> getColumnDescriptors() {
    return columns.keySet();
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, ColumnWriterV2> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().getTotalBufferedSize()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  @Override
  public long getAllocatedSize() {
    long total = 0;
    for (ColumnWriterV2 memColumn : columns.values()) {
      total += memColumn.allocatedSize();
    }
    return total;
  }

  @Override
  public long getBufferedSize() {
    long total = 0;
    for (ColumnWriterV2 memColumn : columns.values()) {
      total += memColumn.getTotalBufferedSize();
    }
    return total;
  }

  @Override
  public void flush() {
    for (ColumnWriterV2 memColumn : columns.values()) {
      long rows = rowCount - memColumn.getRowsWrittenSoFar();
      if (rows > 0) {
        memColumn.writePage(rowCount);
      }
      memColumn.finalizeColumnChunk();
    }
  }

  public String memUsageString() {
    StringBuilder b = new StringBuilder("Store {\n");
    for (ColumnWriterV2 memColumn : columns.values()) {
      b.append(memColumn.memUsageString(" "));
    }
    b.append("}\n");
    return b.toString();
  }

  @Override
  public void endRecord() {
    ++ rowCount;
    if (rowCount >= rowCountForNextSizeCheck) {
      sizeCheck();
    }
  }

  private void sizeCheck() {
    long minRecordToWait = Long.MAX_VALUE;
    for (ColumnWriterV2 writer : writers) {
      long usedMem = writer.getCurrentPageBufferedSize();
      long rows = rowCount - writer.getRowsWrittenSoFar();
      long remainingMem = pageSizeThreshold - usedMem;
      if (remainingMem <= thresholdTolerance) {
        writer.writePage(rowCount);
        remainingMem = pageSizeThreshold;
      }
      long rowsToFillPage =
          usedMem == 0 ?
              MAXIMUM_RECORD_COUNT_FOR_CHECK
              : (long)((float)rows) / usedMem * remainingMem;
      if (rowsToFillPage < minRecordToWait) {
        minRecordToWait = rowsToFillPage;
      }
    }
    if (minRecordToWait == Long.MAX_VALUE) {
      minRecordToWait = MINIMUM_RECORD_COUNT_FOR_CHECK;
    }
    // will check again halfway
    rowCountForNextSizeCheck = rowCount +
        min(
            max(minRecordToWait / 2, MINIMUM_RECORD_COUNT_FOR_CHECK), // no less than MINIMUM_RECORD_COUNT_FOR_CHECK
            MAXIMUM_RECORD_COUNT_FOR_CHECK); // no more than MAXIMUM_RECORD_COUNT_FOR_CHECK
  }

}