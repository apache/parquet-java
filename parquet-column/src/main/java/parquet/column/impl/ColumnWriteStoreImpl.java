/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriteStore;
import parquet.column.ColumnWriter;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.page.PageWriteStore;
import parquet.column.page.PageWriter;


public class ColumnWriteStoreImpl implements ColumnWriteStore {

  private final Map<ColumnDescriptor, ColumnWriterImpl> columns = new TreeMap<ColumnDescriptor, ColumnWriterImpl>();
  private final PageWriteStore pageWriteStore;
  private final int pageSizeThreshold;
  private final int dictionaryPageSizeThreshold;
  private final boolean enableDictionary;
  private final int initialSizePerCol;
  private final WriterVersion writerVersion;

  public ColumnWriteStoreImpl(PageWriteStore pageWriteStore, int pageSizeThreshold, int initialSizePerCol, int dictionaryPageSizeThreshold, boolean enableDictionary, WriterVersion writerVersion) {
    super();
    this.pageWriteStore = pageWriteStore;
    this.pageSizeThreshold = pageSizeThreshold;
    this.initialSizePerCol = initialSizePerCol;
    this.dictionaryPageSizeThreshold = dictionaryPageSizeThreshold;
    this.enableDictionary = enableDictionary;
    this.writerVersion = writerVersion;
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    ColumnWriterImpl column = columns.get(path);
    if (column == null) {
      column = newMemColumn(path);
      columns.put(path, column);
    }
    return column;
  }

  public Set<ColumnDescriptor> getColumnDescriptors() {
    return columns.keySet();
  }

  private ColumnWriterImpl newMemColumn(ColumnDescriptor path) {
    PageWriter pageWriter = pageWriteStore.getPageWriter(path);
    return new ColumnWriterImpl(path, pageWriter, pageSizeThreshold, initialSizePerCol, dictionaryPageSizeThreshold, enableDictionary, writerVersion);
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, ColumnWriterImpl> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().getBufferedSizeInMemory()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  public long allocatedSize() {
    Collection<ColumnWriterImpl> values = columns.values();
    long total = 0;
    for (ColumnWriterImpl memColumn : values) {
      total += memColumn.allocatedSize();
    }
    return total;
  }

  public long memSize() {
    Collection<ColumnWriterImpl> values = columns.values();
    long total = 0;
    for (ColumnWriterImpl memColumn : values) {
      total += memColumn.getBufferedSizeInMemory();
    }
    return total;
  }

  public long maxColMemSize() {
    Collection<ColumnWriterImpl> values = columns.values();
    long max = 0;
    for (ColumnWriterImpl memColumn : values) {
      max = Math.max(max, memColumn.getBufferedSizeInMemory());
    }
    return max;
  }

  @Override
  public void flush() {
    Collection<ColumnWriterImpl> values = columns.values();
    for (ColumnWriterImpl memColumn : values) {
      memColumn.flush();
    }
  }

  public String memUsageString() {
    StringBuilder b = new StringBuilder("Store {\n");
    Collection<ColumnWriterImpl> values = columns.values();
    for (ColumnWriterImpl memColumn : values) {
      b.append(memColumn.memUsageString(" "));
    }
    b.append("}\n");
    return b.toString();
  }

}