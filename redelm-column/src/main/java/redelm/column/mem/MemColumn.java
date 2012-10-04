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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import redelm.Log;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;

public abstract class MemColumn implements ColumnReader, ColumnWriter {

  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;

  private int recordCount;
  private int readRecords;
  protected ByteArrayOutputStream out;
  protected ByteArrayInputStream in;
  private byte[] data;

  private int repetitionLevel;
  private int definitionLevel;
  private boolean valueRead = false;
  private boolean consumed = true;

  public MemColumn(ColumnDescriptor path, int initialSize) {
    this.path = path;
    this.out = new ByteArrayOutputStream(initialSize);
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  private void log(Object value, int r, int d) {
    System.out.println(path+" "+value+" r:"+r+" d:"+d);
  }

  void write(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(String value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    write(value);
    ++recordCount;
  }

  void write(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    out.write(repetitionLevel);
    out.write(definitionLevel);
    ++recordCount;
  }

  private void read() {
    repetitionLevel = in.read();
    definitionLevel = in.read();
    ++readRecords;
    consumed = false;
  }

  public int getCurrentRepetitionLevel() {
    checkRead();
    return repetitionLevel;
  }

  protected void checkRead() {
    if (consumed && !isFullyConsumed()) {
      read();
    } else if (isFullyConsumed()) {
      repetitionLevel = 0;
    }
  }

  protected void checkValueRead() {
    checkRead();
    if (!consumed && !valueRead) {
      readCurrentValue();
      valueRead = true;
    }
  }

  public int getCurrentDefinitionLevel() {
    checkRead();
    return definitionLevel;
  }

  public int size() {
    return recordCount;
  }

  protected abstract void readCurrentValue();

  public void initiateRead() {
    if (data == null) {
      data = out.toByteArray();
      out = null;
    }
    in = new ByteArrayInputStream(data);
    readRecords = 0;
  }

  abstract public String getCurrentValueToString() throws IOException;

  @Override
  public boolean isFullyConsumed() {
    return readRecords >= recordCount;
  }

  @Override
  public void consume() {
    consumed = true;
    valueRead = false;
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBool() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary() {
    throw new UnsupportedOperationException();
  }

  public int memSize() {
    if (out == null) {
      return data.length;
    } else {
      return out.size();
    }
  }

  public byte[] getData() {
    initiateRead();
    return data;
  }

  public ColumnDescriptor getDescriptor() {
    return path;
  }

  public void set(byte[] data, int recordCount) {
    this.data = data;
    this.out = null;
    this.in = new ByteArrayInputStream(data);
    this.readRecords = 0;
    this.recordCount = recordCount;
  }

  public int getRecordCount() {
    return recordCount;
  }
}
