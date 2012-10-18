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

import redelm.Log;
import redelm.column.BytesOutput;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.primitive.PrimitiveColumnWriter;
import redelm.column.primitive.SimplePrimitiveColumnWriter;

final class MemColumnWriter implements ColumnWriter {
  private static final Log LOG = Log.getLog(MemColumn.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;
  private PrimitiveColumnWriter repetitionLevelColumn;
  private PrimitiveColumnWriter definitionLevelColumn;
  private PrimitiveColumnWriter dataColumn;
  private int valueCount;

  public MemColumnWriter(ColumnDescriptor path, int initialSize) {
    this.path = path;
    // 5% each for repetition and definition level
    this.repetitionLevelColumn = new SimplePrimitiveColumnWriter(initialSize/20);
    this.definitionLevelColumn = new SimplePrimitiveColumnWriter(initialSize/20);
    // 90% for the data
    this.dataColumn = new SimplePrimitiveColumnWriter(initialSize*9/10);
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path+" "+value+" r:"+r+" d:"+d);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    ++ valueCount;
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeDouble(value);
    ++ valueCount;
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeFloat(value);
    ++ valueCount;
  }

  @Override
  public void write(byte[] value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeBytes(value);
    ++ valueCount;
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeBoolean(value);
    ++ valueCount;
  }

  @Override
  public void write(String value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeString(value);
    ++ valueCount;
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeByte(repetitionLevel);
    definitionLevelColumn.writeByte(definitionLevel);
    dataColumn.writeInt(value);
    ++ valueCount;
  }


  @Override
  public void writeRepetitionLevelColumn(BytesOutput out) throws IOException {
    repetitionLevelColumn.writeData(out);
  }

  @Override
  public void writeDefinitionLevelColumn(BytesOutput out) throws IOException {
    definitionLevelColumn.writeData(out);
  }

  @Override
  public void writeDataColumn(BytesOutput out) throws IOException {
    dataColumn.writeData(out);
  }

  @Override
  public void reset() {
    repetitionLevelColumn.reset();
    definitionLevelColumn.reset();
    dataColumn.reset();
    valueCount = 0;
  }


  public int memSize() {
    return repetitionLevelColumn.getMemSize()
        + definitionLevelColumn.getMemSize()
        + dataColumn.getMemSize();
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }
}