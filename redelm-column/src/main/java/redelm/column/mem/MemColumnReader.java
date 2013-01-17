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
import java.io.DataInputStream;
import java.io.IOException;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.primitive.BoundedColumnFactory;
import redelm.column.primitive.DevNullColumnReader;
import redelm.column.primitive.PrimitiveColumnReader;
import redelm.column.primitive.SimplePrimitiveColumnReader;

abstract class MemColumnReader implements ColumnReader {

  private int repetitionLevel;
  private int definitionLevel;
  private int readValues;
  private int valueCount;
  private boolean valueRead = false;
  private boolean consumed = true;
  private PrimitiveColumnReader repetitionLevelColumn;
  private PrimitiveColumnReader definitionLevelColumn;
  protected PrimitiveColumnReader dataColumn;
  private final ColumnDescriptor path;

  public MemColumnReader(ColumnDescriptor path) {
    this.path = path;
  }

  @Override
  public boolean isFullyConsumed() {
    return readValues >= valueCount;
  }

  @Override
  public String getString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInteger() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean() {
    throw new UnsupportedOperationException();
  }

  public long getLong() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCurrentRepetitionLevel() {
    checkRead();
    return repetitionLevel;
  }

  abstract public String getCurrentValueToString() throws IOException;

  protected abstract void readCurrentValue();

  protected void checkValueRead() {
    checkRead();
    if (!consumed && !valueRead) {
      readCurrentValue();
      valueRead = true;
    }
  }

  @Override
  public int getCurrentDefinitionLevel() {
    checkRead();
    return definitionLevel;
  }

  // TODO: change the logic around read() to not tie together reading from the 3 columns
  private void read() {
    repetitionLevel = repetitionLevelColumn.readInteger();
    definitionLevel = definitionLevelColumn.readInteger();
    ++readValues;
    consumed = false;
  }

  protected void checkRead() {
    if (consumed && !isFullyConsumed()) {
      read();
    } else if (isFullyConsumed()) {
      repetitionLevel = 0;
    }
  }

  @Override
  public void consume() {
    consumed = true;
    valueRead = false;
  }

  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  public void initRepetitionLevelColumn(byte[] bytes, int index, int length) {
    repetitionLevelColumn = BoundedColumnFactory.getBoundedReader(path.getRepetitionLevel());
    try {
      repetitionLevelColumn.readStripe(new DataInputStream(new ByteArrayInputStream(bytes, index, length)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initDefinitionLevelColumn(byte[] bytes, int index, int length) {
    definitionLevelColumn = BoundedColumnFactory.getBoundedReader(path.getDefinitionLevel());
    try {
      definitionLevelColumn.readStripe(new DataInputStream(new ByteArrayInputStream(bytes, index, length)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initDataColumn(byte[] bytes, int index, int length) {
    dataColumn = new SimplePrimitiveColumnReader();
    try {
      dataColumn.readStripe(new DataInputStream(new ByteArrayInputStream(bytes, index, length)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}