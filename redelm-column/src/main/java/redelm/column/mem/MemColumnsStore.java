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
import redelm.schema.PrimitiveType.Primitive;

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
      try {
        currentInt = in.readInt();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(int value) {
       try {
        out.writeInt(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentInt);
    }
  }

  private static final class BINARYMemColumn extends MemColumn {
    private byte[] current;

    public BINARYMemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public byte[] getBinary() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      try {
        int length = in.readInt();
        if (length > 0) {
          byte[] b = new byte[length];
          int o = 0;
          do {
            o = o + in.read(b, o, length - o);
          } while (o < length);
          current = b;
        } else {
          current = new byte[0];
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(byte[] bytes) {
      try {
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
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
      try {
        currentString = in.readUTF();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(String value) {
      try {
        out.writeUTF(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return currentString;
    }
  }
  private static final class BOOLMemColumn extends MemColumn {
    private boolean current;

    public BOOLMemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public boolean getBool() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      try {
        current = in.readBoolean();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(boolean value) {
      try {
        out.writeBoolean(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

  private static final class FLOATMemColumn extends MemColumn {
    private float current;

    public FLOATMemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public float getFloat() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      try {
        current = in.readFloat();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(float value) {
      try {
        out.writeFloat(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

  private static final class DOUBLEMemColumn extends MemColumn {
    private double current;

    public DOUBLEMemColumn(ColumnDescriptor path, int initialSize) {
      super(path, initialSize);
    }

    @Override
    public double getDouble() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      try {
        current = in.readDouble();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void write(double value) {
      try {
        out.writeDouble(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
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
    case BOOL:
      return new BOOLMemColumn(path, initialColumnSize);
    case BINARY:
      return new BINARYMemColumn(path, initialColumnSize);
    case FLOAT:
      return new FLOATMemColumn(path, initialColumnSize);
    case DOUBLE:
      return new DOUBLEMemColumn(path, initialColumnSize);
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

  public Collection<MemColumn> getColumns() {
    return columns.values();
  }

  public void setColumn(String[] path, Primitive primitive, byte[] data, int recordCount) {
    ColumnDescriptor descriptor = new ColumnDescriptor(path, primitive);
    MemColumn col = getColumn(descriptor);
    col.set(data, recordCount);
    columns.put(descriptor, col);

  }

  public boolean isFullyConsumed() {
    for (MemColumn c : columns.values()) {
      if (c.isFullyConsumed()) {
        return true;
      }

    }
    return false;
  }
}
