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
import redelm.column.ColumnWriteStore;
import redelm.column.ColumnWriter;

public class MemColumnWriteStore implements ColumnWriteStore {

  private final Map<ColumnDescriptor, MemColumnWriter> columns = new TreeMap<ColumnDescriptor, MemColumnWriter>();
  private final PageWriteStore pageWriteStore;
  private final int pageSizeThreshold;

  public MemColumnWriteStore(PageWriteStore pageWriteStore, int pageSizeThreshold) {
    super();
    this.pageWriteStore = pageWriteStore;
    this.pageSizeThreshold = pageSizeThreshold;
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    MemColumnWriter column = columns.get(path);
    if (column == null) {
      column = newMemColumn(path);
      columns.put(path, column);
    }
    return column;
  }

  private MemColumnWriter newMemColumn(ColumnDescriptor path) {
    PageWriter pageWriter = pageWriteStore.getPageWriter(path);
    return new MemColumnWriter(path, pageWriter, pageSizeThreshold);
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, MemColumnWriter> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().memSize()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  public int memSize() {
    Collection<MemColumnWriter> values = columns.values();
    int total = 0;
    for (MemColumnWriter memColumn : values) {
      total += memColumn.memSize();
    }
    return total;
  }

  public long maxColMemSize() {
    Collection<MemColumnWriter> values = columns.values();
    long max = 0;
    for (MemColumnWriter memColumn : values) {
      max = Math.max(max, memColumn.memSize());
    }
    return max;
  }

  @Override
  public void flush() {
    Collection<MemColumnWriter> values = columns.values();
    for (MemColumnWriter memColumn : values) {
      memColumn.flush();
    }
  }

}