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
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;

public class MemColumn {
  private static final Log LOG = Log.getLog(MemColumn.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;

  private MemColumnWriter memColumnWriter;

  public MemColumn(ColumnDescriptor path, int initialSize, PageWriter pageWriter, int pageSizeThreshold) {
    this.path = path;
    this.memColumnWriter = new MemColumnWriter(path, initialSize, pageWriter, pageSizeThreshold);
  }

  public ColumnDescriptor getDescriptor() {
    return path;
  }

  public ColumnWriter getColumnWriter() {
    return memColumnWriter;
  }

  private MemColumnReader newMemColumnReader(PageReader pageReader) {
    switch (path.getType()) {
    case INT32:
      return new INT32MemColumnReader(path, pageReader);
    case INT64:
      return new INT64MemColumnReader(path, pageReader);
    case STRING:
      return new STRINGMemColumnReader(path, pageReader);
    case BOOLEAN:
      return new BOOLEANMemColumnReader(path, pageReader);
    case BINARY:
      return new BINARYMemColumnReader(path, pageReader);
    case FLOAT:
      return new FLOATMemColumnReader(path, pageReader);
    case DOUBLE:
      return new DOUBLEMemColumnReader(path, pageReader);
    }
    throw new RuntimeException("type "+path.getType()+" not supported");
  }

  public ColumnReader getColumnReader(PageReader pageReader) {
    return newMemColumnReader(pageReader);
  }

  private static final class INT32MemColumnReader extends MemColumnReader {
    private int currentInt;

    public INT32MemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
    }

    @Override
    public int getInteger() {
      checkValueRead();
      return currentInt;
    }

    @Override
    protected void readCurrentValue() {
        currentInt = dataColumn.readInteger();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentInt);
    }
  }

  private static final class INT64MemColumnReader extends MemColumnReader {
    private long currentLong;

    public INT64MemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
    }

    @Override
    public long getLong() {
      checkValueRead();
      return currentLong;
    }

    @Override
    protected void readCurrentValue() {
      currentLong = dataColumn.readLong();
    }

    @Override
    public String getCurrentValueToString() throws IOException {
      checkRead();
      return String.valueOf(currentLong);
    }
  }

  private static final class BINARYMemColumnReader extends MemColumnReader {
    private byte[] current;

    public BINARYMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
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

    public STRINGMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
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
  private static final class BOOLEANMemColumnReader extends MemColumnReader {
    private boolean current;

    public BOOLEANMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
    }

    @Override
    public boolean getBoolean() {
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

    public FLOATMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
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

    public DOUBLEMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
      super(path, pageReader);
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

  public long getMemSize() {
    return memColumnWriter.memSize();
  }
}