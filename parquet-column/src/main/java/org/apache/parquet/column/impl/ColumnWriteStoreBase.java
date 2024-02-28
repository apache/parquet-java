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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.unmodifiableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.schema.MessageType;

/**
 * Base implementation for {@link ColumnWriteStore} to be extended to specialize for V1 and V2 pages.
 */
abstract class ColumnWriteStoreBase implements ColumnWriteStore {

  // Used to support the deprecated workflow of ColumnWriteStoreV1 (lazy init of ColumnWriters)
  private interface ColumnWriterProvider {
    ColumnWriter getColumnWriter(ColumnDescriptor path);
  }

  private final ColumnWriterProvider columnWriterProvider;

  // will flush even if size bellow the threshold by this much to facilitate page alignment
  private static final float THRESHOLD_TOLERANCE_RATIO = 0.1f; // 10 %

  private final Map<ColumnDescriptor, ColumnWriterBase> columns;
  private final ParquetProperties props;
  private final long thresholdTolerance;
  private long rowCount;
  private long rowCountForNextSizeCheck;
  private StatusManager statusManager = StatusManager.create();

  // To be used by the deprecated constructor of ColumnWriteStoreV1
  @Deprecated
  ColumnWriteStoreBase(final PageWriteStore pageWriteStore, final ParquetProperties props) {
    this.props = props;
    this.thresholdTolerance = (long) (props.getPageSizeThreshold() * THRESHOLD_TOLERANCE_RATIO);

    this.columns = new TreeMap<>();

    this.rowCountForNextSizeCheck = min(props.getMinRowCountForPageSizeCheck(), props.getPageRowCountLimit());

    columnWriterProvider = new ColumnWriterProvider() {
      @Override
      public ColumnWriter getColumnWriter(ColumnDescriptor path) {
        ColumnWriterBase column = columns.get(path);
        if (column == null) {
          column = createColumnWriterBase(path, pageWriteStore.getPageWriter(path), null, props);
          columns.put(path, column);
        }
        return column;
      }
    };
  }

  ColumnWriteStoreBase(MessageType schema, PageWriteStore pageWriteStore, ParquetProperties props) {
    this.props = props;
    this.thresholdTolerance = (long) (props.getPageSizeThreshold() * THRESHOLD_TOLERANCE_RATIO);
    Map<ColumnDescriptor, ColumnWriterBase> mcolumns = new TreeMap<>();
    for (ColumnDescriptor path : schema.getColumns()) {
      PageWriter pageWriter = pageWriteStore.getPageWriter(path);
      mcolumns.put(path, createColumnWriterBase(path, pageWriter, null, props));
    }
    this.columns = unmodifiableMap(mcolumns);

    this.rowCountForNextSizeCheck = min(props.getMinRowCountForPageSizeCheck(), props.getPageRowCountLimit());

    columnWriterProvider = new ColumnWriterProvider() {
      @Override
      public ColumnWriter getColumnWriter(ColumnDescriptor path) {
        return columns.get(path);
      }
    };
  }

  // The Bloom filter is written to a specified bitset instead of pages, so it needs a separate write store abstract.
  ColumnWriteStoreBase(
      MessageType schema,
      PageWriteStore pageWriteStore,
      BloomFilterWriteStore bloomFilterWriteStore,
      ParquetProperties props) {
    this.props = props;
    this.thresholdTolerance = (long) (props.getPageSizeThreshold() * THRESHOLD_TOLERANCE_RATIO);
    Map<ColumnDescriptor, ColumnWriterBase> mcolumns = new TreeMap<>();
    for (ColumnDescriptor path : schema.getColumns()) {
      PageWriter pageWriter = pageWriteStore.getPageWriter(path);
      if (props.isBloomFilterEnabled(path)) {
        BloomFilterWriter bloomFilterWriter = bloomFilterWriteStore.getBloomFilterWriter(path);
        mcolumns.put(path, createColumnWriterBase(path, pageWriter, bloomFilterWriter, props));
      } else {
        mcolumns.put(path, createColumnWriterBase(path, pageWriter, null, props));
      }
    }
    this.columns = unmodifiableMap(mcolumns);

    this.rowCountForNextSizeCheck = props.getMinRowCountForPageSizeCheck();

    columnWriterProvider = new ColumnWriterProvider() {
      @Override
      public ColumnWriter getColumnWriter(ColumnDescriptor path) {
        return columns.get(path);
      }
    };
  }

  private ColumnWriterBase createColumnWriterBase(
      ColumnDescriptor path, PageWriter pageWriter, BloomFilterWriter bloomFilterWriter, ParquetProperties props) {
    ColumnWriterBase columnWriterBase = createColumnWriter(path, pageWriter, bloomFilterWriter, props);
    columnWriterBase.initStatusManager(statusManager);
    return columnWriterBase;
  }

  abstract ColumnWriterBase createColumnWriter(
      ColumnDescriptor path, PageWriter pageWriter, BloomFilterWriter bloomFilterWriter, ParquetProperties props);

  @Override
  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    return columnWriterProvider.getColumnWriter(path);
  }

  public Set<ColumnDescriptor> getColumnDescriptors() {
    return columns.keySet();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Entry<ColumnDescriptor, ColumnWriterBase> entry : columns.entrySet()) {
      sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
      sb.append(entry.getValue().getTotalBufferedSize()).append(" bytes");
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public long getAllocatedSize() {
    long total = 0;
    for (ColumnWriterBase memColumn : columns.values()) {
      total += memColumn.allocatedSize();
    }
    return total;
  }

  @Override
  public long getBufferedSize() {
    long total = 0;
    for (ColumnWriterBase memColumn : columns.values()) {
      total += memColumn.getTotalBufferedSize();
    }
    return total;
  }

  @Override
  public void flush() {
    for (ColumnWriterBase memColumn : columns.values()) {
      long rows = rowCount - memColumn.getRowsWrittenSoFar();
      if (rows > 0) {
        memColumn.writePage();
      }
      memColumn.finalizeColumnChunk();
    }
  }

  @Override
  public String memUsageString() {
    StringBuilder b = new StringBuilder("Store {\n");
    for (ColumnWriterBase memColumn : columns.values()) {
      b.append(memColumn.memUsageString(" "));
    }
    b.append("}\n");
    return b.toString();
  }

  public long maxColMemSize() {
    long max = 0;
    for (ColumnWriterBase memColumn : columns.values()) {
      max = Math.max(max, memColumn.getBufferedSizeInMemory());
    }
    return max;
  }

  @Override
  public void close() {
    flush(); // calling flush() here to keep it consistent with the behavior before merging with master
    for (ColumnWriterBase memColumn : columns.values()) {
      memColumn.close();
    }
  }

  @Override
  public void endRecord() {
    ++rowCount;
    if (rowCount >= rowCountForNextSizeCheck) {
      sizeCheck();
    }
  }

  private void sizeCheck() {
    long minRecordToWait = Long.MAX_VALUE;
    int pageRowCountLimit = props.getPageRowCountLimit();
    long rowCountForNextRowCountCheck = rowCount + pageRowCountLimit;
    for (ColumnWriterBase writer : columns.values()) {
      long usedMem = writer.getCurrentPageBufferedSize();
      long rows = rowCount - writer.getRowsWrittenSoFar();
      long remainingMem = props.getPageSizeThreshold() - usedMem;
      if (remainingMem <= thresholdTolerance
          || rows >= pageRowCountLimit
          || writer.getValueCount() >= props.getPageValueCountThreshold()) {
        writer.writePage();
        remainingMem = props.getPageSizeThreshold();
      } else {
        rowCountForNextRowCountCheck =
            min(rowCountForNextRowCountCheck, writer.getRowsWrittenSoFar() + pageRowCountLimit);
      }
      // estimate remaining row count by previous input for next row count check
      long rowsToFillPage = usedMem == 0 ? props.getMaxRowCountForPageSizeCheck() : rows * remainingMem / usedMem;
      if (rowsToFillPage < minRecordToWait) {
        minRecordToWait = rowsToFillPage;
      }
    }
    if (minRecordToWait == Long.MAX_VALUE) {
      minRecordToWait = props.getMinRowCountForPageSizeCheck();
    }

    if (props.estimateNextSizeCheck()) {
      // will check again halfway if between min and max
      rowCountForNextSizeCheck = rowCount
          + min(
              max(minRecordToWait / 2, props.getMinRowCountForPageSizeCheck()),
              props.getMaxRowCountForPageSizeCheck());
    } else {
      rowCountForNextSizeCheck = rowCount + props.getMinRowCountForPageSizeCheck();
    }

    // Do the check earlier if required to keep the row count limit
    if (rowCountForNextRowCountCheck < rowCountForNextSizeCheck) {
      rowCountForNextSizeCheck = rowCountForNextRowCountCheck;
    }
  }

  @Override
  public boolean isColumnFlushNeeded() {
    return rowCount + 1 >= rowCountForNextSizeCheck;
  }
}
