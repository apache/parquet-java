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

import redelm.Log;
import redelm.column.BytesOutput;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;

public class MemColumn {
  private static final Log LOG = Log.getLog(MemColumn.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;

  private MemColumnWriter memColumnWriter;
  private MemColumnReader memColumnReader;

  public MemColumn(ColumnDescriptor path, int initialSize) {
    this.path = path;
    this.memColumnWriter = new MemColumnWriter(path, initialSize);
  }

  public ColumnDescriptor getDescriptor() {
    return path;
  }

  public ColumnWriter getColumnWriter() {
    if (memColumnWriter == null) {
      throw new IllegalStateException("now in read mode");
    }
    return memColumnWriter;
  }

  private MemColumnReader newMemColumnReader() {
    switch (path.getType()) {
    case INT64:
      return new INT64MemColumnReader(path);
    case STRING:
      return new STRINGMemColumnReader(path);
    case BOOL:
      return new BOOLMemColumnReader(path);
    case BINARY:
      return new BINARYMemColumnReader(path);
    case FLOAT:
      return new FLOATMemColumnReader(path);
    case DOUBLE:
      return new DOUBLEMemColumnReader(path);
    }
    throw new RuntimeException("type "+path.getType()+" not supported");
  }

  public void setForRead(int valueCount, byte[] repetitionLevels, byte[] definitionLevel, byte[] data) {
    memColumnReader = newMemColumnReader();
    memColumnReader.initRepetitionLevelColumn(repetitionLevels, 0, repetitionLevels.length);
    memColumnReader.initDefinitionLevelColumn(definitionLevel, 0, definitionLevel.length);
    memColumnReader.initDataColumn(data, 0, data.length);
    memColumnReader.setValueCount(valueCount);
  }

  public void flip() {
    if (memColumnWriter != null) {
      try {
        memColumnReader = newMemColumnReader();
        memColumnReader.setValueCount(memColumnWriter.getValueCount());
        memColumnWriter.writeRepetitionLevelColumn(new BytesOutput() {
          public void write(byte[] bytes, int index, int length) throws IOException {
            memColumnReader.initRepetitionLevelColumn(bytes, index, length);
          }
        });
        memColumnWriter.writeDefinitionLevelColumn(new BytesOutput() {
          public void write(byte[] bytes, int index, int length) throws IOException {
            memColumnReader.initDefinitionLevelColumn(bytes, index, length);
          }
        });
        memColumnWriter.writeDataColumn(new BytesOutput() {
          public void write(byte[] bytes, int index, int length) throws IOException {
            memColumnReader.initDataColumn(bytes, index, length);
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("nothing to flip");
    }
  }

  public ColumnReader getColumnReader() {
    if (memColumnReader == null) {
      throw new IllegalStateException("not in read mode");
    }
    return memColumnReader;
  }

  public int memSize() {
    if (memColumnWriter != null) {
      return memColumnWriter.memSize();
    }
    throw new RuntimeException("Not writing anymore");
  }

  private static final class INT64MemColumnReader extends MemColumnReader {
    private int currentInt;

    public INT64MemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public int getInt() {
      checkValueRead();
      return currentInt;
    }

    @Override
    protected void readCurrentValue() {
        currentInt = dataColumn.readInt();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentInt);
    }
  }

  private static final class BINARYMemColumnReader extends MemColumnReader {
    private byte[] current;

    public BINARYMemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public byte[] getBinary() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readBytes();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

  private static final class STRINGMemColumnReader extends MemColumnReader {
    private static final Charset ENCODING = Charset.forName("UTF-8");
    private String currentString;

    public STRINGMemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public String getString() {
      checkValueRead();
      return currentString;
    }

    @Override
    protected void readCurrentValue() {
      currentString = dataColumn.readString();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return currentString;
    }
  }
  private static final class BOOLMemColumnReader extends MemColumnReader {
    private boolean current;

    public BOOLMemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public boolean getBool() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readBoolean();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

  private static final class FLOATMemColumnReader extends MemColumnReader {
    private float current;

    public FLOATMemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public float getFloat() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readFloat();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

  private static final class DOUBLEMemColumnReader extends MemColumnReader {
    private double current;

    public DOUBLEMemColumnReader(ColumnDescriptor path) {
      super(path);
    }

    @Override
    public double getDouble() {
      checkValueRead();
      return current;
    }

    @Override
    protected void readCurrentValue() {
      current = dataColumn.readDouble();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(current);
    }
  }

}
