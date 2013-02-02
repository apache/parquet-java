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
package redelm.column.mem;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;
import redelm.schema.MessageType;

public class MemColumnsStore extends ColumnsStore {

  private final Map<ColumnDescriptor, MemColumn> columns = new TreeMap<ColumnDescriptor, MemColumn>();
  private final int initialColumnSize;
  private final PageStore pageStore;
  private final int pageSizeThreshold;

  public MemColumnsStore(int initialColumnSize, PageStore pageStore, int pageSizeThreshold) {
    super();
    this.initialColumnSize = initialColumnSize;
    this.pageStore = pageStore;
    this.pageSizeThreshold = pageSizeThreshold;
  }

  public ColumnReader getColumnReader(ColumnDescriptor path) {
    return getColumn(path).getColumnReader(pageStore.getPageReader(path));
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    return getColumn(path).getColumnWriter();
  }

  private MemColumn getColumn(ColumnDescriptor path) {
    MemColumn column = columns.get(path);
    if (column == null) {
      column = newMemColumn(path);
      columns.put(path, column);
    }
    return column;
  }

  private MemColumn newMemColumn(ColumnDescriptor path) {
    PageWriter pageWriter = pageStore.getPageWriter(path);
    return new MemColumn(path, initialColumnSize, pageWriter, pageSizeThreshold);
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, MemColumn> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().getMemSize()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  public int memSize() {
    Collection<MemColumn> values = columns.values();
    int total = 0;
    for (MemColumn memColumn : values) {
      total += memColumn.getMemSize();
    }
    return total;
  }

  public long maxColMemSize() {
    Collection<MemColumn> values = columns.values();
    long max = 0;
    for (MemColumn memColumn : values) {
      max = Math.max(max, memColumn.getMemSize());
    }
    return max;
  }

  @Override
  public void flush() {
    Collection<MemColumn> values = columns.values();
    for (MemColumn memColumn : values) {
      memColumn.getColumnWriter().flush();
    }
  }

}