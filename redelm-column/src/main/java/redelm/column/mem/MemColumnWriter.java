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
import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.primitive.BitPackingColumnWriter;
import redelm.column.primitive.BooleanPlainColumnReader;
import redelm.column.primitive.BooleanPlainColumnWriter;
import redelm.column.primitive.BoundedColumnFactory;
import redelm.column.primitive.PrimitiveColumnWriter;
import redelm.column.primitive.PlainColumnWriter;

final class MemColumnWriter implements ColumnWriter {
  private static final Log LOG = Log.getLog(MemColumnWriter.class);
  private static final boolean DEBUG = false; //Log.DEBUG;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private final long pageSizeThreshold;
  private PrimitiveColumnWriter repetitionLevelColumn;
  private PrimitiveColumnWriter definitionLevelColumn;
  private PrimitiveColumnWriter dataColumn;
  private int valueCount;

  public MemColumnWriter(ColumnDescriptor path, int initialSize, PageWriter pageWriter, int pageSizeThreshold) {
    this.path = path;
    this.pageWriter = pageWriter;
    this.pageSizeThreshold = pageSizeThreshold;
    repetitionLevelColumn = new BitPackingColumnWriter(path.getRepetitionLevel());
    definitionLevelColumn = BoundedColumnFactory.getBoundedWriter(path.getDefinitionLevel());
    switch (path.getType()) {
    case BOOLEAN:
      this.dataColumn = new BooleanPlainColumnWriter(initialSize);
    default:
      this.dataColumn = new PlainColumnWriter(initialSize);
    }
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path+" "+value+" r:"+r+" d:"+d);
  }

  private void accountForValueWritten() {
    ++ valueCount;
    long memSize = repetitionLevelColumn.getMemSize()
        + definitionLevelColumn.getMemSize()
        + dataColumn.getMemSize();
    if (memSize > pageSizeThreshold) {
      writePage();
    }
  }

  private void writePage() {
    if (DEBUG) LOG.debug("write page");
    try {
      pageWriter.writePage(BytesInput.fromSequence(repetitionLevelColumn.getBytes(), definitionLevelColumn.getBytes(), dataColumn.getBytes()), valueCount);
    } catch (IOException e) {
      // TODO: cleanup
      throw new RuntimeException(e);
    }
    repetitionLevelColumn.reset();
    definitionLevelColumn.reset();
    dataColumn.reset();
    valueCount = 0;
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    accountForValueWritten();
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeDouble(value);
    accountForValueWritten();
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeFloat(value);
    accountForValueWritten();
  }

  @Override
  public void write(byte[] value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeBytes(value);
    accountForValueWritten();
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeBoolean(value);
    accountForValueWritten();
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeInteger(value);
    accountForValueWritten();
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeLong(value);
    accountForValueWritten();
  }

  @Override
  public void flush() {
    if (valueCount > 0) {
      writePage();
    }
  }

  @Override
  public long memSize() {
    return repetitionLevelColumn.getMemSize()
        + definitionLevelColumn.getMemSize()
        + dataColumn.getMemSize()
        + pageWriter.getMemSize();
  }
}