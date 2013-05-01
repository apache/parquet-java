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
package parquet.column.impl;

import static parquet.bytes.BytesInput.concat;

import java.io.IOException;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriter;
import parquet.column.page.PageWriter;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;


final class ColumnWriterImpl implements ColumnWriter {
  private static final Log LOG = Log.getLog(ColumnWriterImpl.class);
  private static final boolean DEBUG = false; //Log.DEBUG;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private final long pageSizeThreshold;
  private ValuesWriter repetitionLevelColumn;
  private ValuesWriter definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;

  public ColumnWriterImpl(ColumnDescriptor path, PageWriter pageWriter, int pageSizeThreshold) {
    this.path = path;
    this.pageWriter = pageWriter;
    this.pageSizeThreshold = pageSizeThreshold;
    repetitionLevelColumn = new ByteBitPackingValuesWriter(path.getMaxRepetitionLevel());
    definitionLevelColumn = new ByteBitPackingValuesWriter(path.getMaxDefinitionLevel());
    switch (path.getType()) {
    case BOOLEAN:
      this.dataColumn = new BooleanPlainValuesWriter(pageSizeThreshold / 10 * 11);
      break;
    default:
      this.dataColumn = new PlainValuesWriter(pageSizeThreshold / 10 * 11);
    }
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path+" "+value+" r:"+r+" d:"+d);
  }

  private void accountForValueWritten() {
    ++ valueCount;
    long memSize = repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize();
    if (memSize > pageSizeThreshold) {
      writePage();
    }
  }

  private void writePage() {
    if (DEBUG) LOG.debug("write page");
    try {
      pageWriter.writePage(
          concat(repetitionLevelColumn.getBytes(), definitionLevelColumn.getBytes(), dataColumn.getBytes()),
          valueCount,
          repetitionLevelColumn.getEncoding(),
          definitionLevelColumn.getEncoding(),
          dataColumn.getEncoding());
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page for " + path, e);
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
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
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
  public long getBufferedSizeInMemory() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + pageWriter.getMemSize();
  }

  public long allocatedSize() {
    return repetitionLevelColumn.getAllocatedSize()
    + definitionLevelColumn.getAllocatedSize()
    + dataColumn.getAllocatedSize()
    + pageWriter.allocatedSize();
  }
}
