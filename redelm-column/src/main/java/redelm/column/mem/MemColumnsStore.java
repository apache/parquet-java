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
import redelm.schema.PrimitiveType.Primitive;

public class MemColumnsStore extends ColumnsStore {

  Map<ColumnDescriptor, MemColumn> columns = new TreeMap<ColumnDescriptor, MemColumn>();
  private int initialColumnSize;
  private MessageType schema;

  public MemColumnsStore(int initialColumnSize, MessageType schema) {
    super();
    this.initialColumnSize = initialColumnSize;
    this.schema = schema;
  }

  public ColumnReader getColumnReader(ColumnDescriptor path) {
    return getColumn(path).getColumnReader();
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
    return new MemColumn(path, initialColumnSize);
  }

  @Override
  public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Entry<ColumnDescriptor, MemColumn> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().memSize()).append(" bytes");
        sb.append("\n");
      }
      return sb.toString();
  }

  public int memSize() {
    Collection<MemColumn> values = columns.values();
    int total = 0;
    for (MemColumn memColumn : values) {
      total += memColumn.memSize();
    }
    return total;
  }

  public int maxColMemSize() {
    Collection<MemColumn> values = columns.values();
    int max = 0;
    for (MemColumn memColumn : values) {
      max = Math.max(max, memColumn.memSize());
    }
    return max;
  }

  public Collection<MemColumn> getColumns() {
    return columns.values();
  }

  public void setForRead(
      String[] path, Primitive primitive, //TODO can eliminate the primitive?
      int recordCount,
      byte[] repetitionLevels,
      byte[] definitionLevels, byte[] data) {
    ColumnDescriptor descriptor = schema.getColumnDescription(path);
    MemColumn col = getColumn(descriptor);
    col.setForRead(recordCount, repetitionLevels, definitionLevels, data);
    columns.put(descriptor, col);
  }

  public boolean isFullyConsumed() {
    for (MemColumn c : columns.values()) {
      if (c.getColumnReader().isFullyConsumed()) {
        return true;
      }
    }
    return false;
  }

  public void flip() {
    for (MemColumn c : columns.values()) {
      c.flip();
    }
  }
}