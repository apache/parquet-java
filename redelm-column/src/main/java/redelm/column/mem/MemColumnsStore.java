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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;


public class MemColumnsStore extends ColumnsStore {
  private static final class INT64MemColumn extends MemColumn {
    private int currentInt;

    public INT64MemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public int getInt() {
      checkValueRead();
      return currentInt;
    }

    @Override
    protected void readCurrentValue() {
      currentInt = in.read();
    }

    @Override
    protected void write(int value) {
       out.write(value);
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentInt);
    }
  }

  private static final class STRINGMemColumn extends MemColumn {
    private static final Charset ENCODING = Charset.forName("UTF-8");
    private String currentString;

    public STRINGMemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public String getString() {
      checkValueRead();
      return currentString;
    }

    @Override
    protected void readCurrentValue() {
      int length = in.read();
      if (length > 0) {
        byte[] b = new byte[length];
        int o = 0;
        do {
          o = o + in.read(b, o, length - o);
        } while (o < length);
        currentString = new String(b, ENCODING);
      } else {
        currentString = "";
      }
    }

    @Override
    protected void write(String value) {

      byte[] bytes = value.getBytes(ENCODING);
      out.write(bytes.length);
      out.write(bytes, 0, bytes.length);
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return currentString;
    }
  }

  Map<ColumnDescriptor, MemColumn> columns = new TreeMap<ColumnDescriptor, MemColumn>();
  private int initialColumnSize;

  public MemColumnsStore(int initialColumnSize) {
    super();
    this.initialColumnSize = initialColumnSize;
  }

  public ColumnReader getColumnReader(ColumnDescriptor path) {
    MemColumn column = getColumn(path);
    column.initiateRead();
    return column;
  }

  public ColumnWriter getColumnWriter(ColumnDescriptor path) {
    return getColumn(path);
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
    switch (path.getType()) {
    case INT64:
      return new INT64MemColumn(path, initialColumnSize);
    case STRING:
      return new STRINGMemColumn(path, initialColumnSize);
    }
    throw new RuntimeException("type "+path.getType()+" not supported");
  }

  public Collection<ColumnReader> getColumnReaders() {
    Collection<ColumnReader> result = new ArrayList<ColumnReader>(columns.size());
    for (MemColumn c : columns.values()) {
      result.add(c);
    }
    return result;
  }

  @Override
  public String toString() {
      StringBuffer sb = new StringBuffer();
      for (Entry<ColumnDescriptor, MemColumn> entry : columns.entrySet()) {
        sb.append(Arrays.toString(entry.getKey().getPath())).append(": ");
        sb.append(entry.getValue().size()).append(" recs ");
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
}
