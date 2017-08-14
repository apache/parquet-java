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

import java.util.*;
import java.util.Map.Entry;

import org.apache.parquet.column.values.bloom.BloomDataWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;

public class ColumnWriteStoreV1 implements ColumnWriteStore {

  private final Map<ColumnDescriptor, ColumnWriterV1> columns = new TreeMap<ColumnDescriptor, ColumnWriterV1>();
  private final PageWriteStore pageWriteStore;
  private final ParquetProperties props;

  public ColumnWriteStoreV1(PageWriteStore pageWriteStore,
                            ParquetProperties props) {
    this.pageWriteStore = pageWriteStore;
    this.props = props;
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    ColumnWriterV1 column = columns.get(path);
    if (column == null) {
      column = newMemColumn(path);
      columns.put(path, column);
    }
    return column;
  }

  public Set<ColumnDescriptor> getColumnDescriptors() {
    return columns.keySet();
  }

  private ColumnWriterV1 newMemColumn(ColumnDescriptor path) {
    PageWriter pageWriter = pageWriteStore.getPageWriter(path);

    if (props.isBloomFilterEnabled() && props.getBloomFilterColumnNames() != null) {
      BloomDataWriter bloomDataWriter = pageWriteStore.getBloomDataWriter(path);
      return new ColumnWriterV1(path, pageWriter, bloomDataWriter, props);
    } else {
      return new ColumnWriterV1(path, pageWriter, props);
    }
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, ColumnWriterV1> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().getBufferedSizeInMemory()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  @Override
  public long getAllocatedSize() {
    Collection<ColumnWriterV1> values = columns.values();
    long total = 0;
    for (ColumnWriterV1 memColumn : values) {
      total += memColumn.allocatedSize();
    }
    return total;
  }

  @Override
  public long getBufferedSize() {
    Collection<ColumnWriterV1> values = columns.values();
    long total = 0;
    for (ColumnWriterV1 memColumn : values) {
      total += memColumn.getBufferedSizeInMemory();
    }
    return total;
  }

  @Override
  public String memUsageString() {
    StringBuilder b = new StringBuilder("Store {\n");
    Collection<ColumnWriterV1> values = columns.values();
    for (ColumnWriterV1 memColumn : values) {
      b.append(memColumn.memUsageString(" "));
    }
    b.append("}\n");
    return b.toString();
  }

  public long maxColMemSize() {
    Collection<ColumnWriterV1> values = columns.values();
    long max = 0;
    for (ColumnWriterV1 memColumn : values) {
      max = Math.max(max, memColumn.getBufferedSizeInMemory());
    }
    return max;
  }

  @Override
  public void flush() {
    Collection<ColumnWriterV1> values = columns.values();
    for (ColumnWriterV1 memColumn : values) {
      memColumn.flush();
    }
  }

  @Override
  public void endRecord() {
    // V1 does not take record boundaries into account
  }

  public void close() {
    Collection<ColumnWriterV1> values = columns.values();
    for (ColumnWriterV1 memColumn : values) {
      memColumn.close();
    }
  }

}
