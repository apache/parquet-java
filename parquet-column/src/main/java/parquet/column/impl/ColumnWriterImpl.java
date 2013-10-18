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
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriter;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriter;
import parquet.column.values.ValuesWriter;
import parquet.column.values.boundedint.DevNullValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 *
 * @author Julien Le Dem
 *
 */
final class ColumnWriterImpl implements ColumnWriter {
  private static final Log LOG = Log.getLog(ColumnWriterImpl.class);
  private static final boolean DEBUG = Log.DEBUG;
  private static final int INITIAL_COUNT_FOR_SIZE_CHECK = 100;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private final long pageSizeThreshold;
  private ValuesWriter repetitionLevelColumn;
  private ValuesWriter definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;
  private int valueCountForNextSizeCheck;

  public ColumnWriterImpl(
      ColumnDescriptor path,
      PageWriter pageWriter,
      int pageSizeThreshold,
      int initialSizePerCol,
      int dictionaryPageSizeThreshold,
      boolean enableDictionary) {
    this.path = path;
    this.pageWriter = pageWriter;
    this.pageSizeThreshold = pageSizeThreshold;
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountForNextSizeCheck = INITIAL_COUNT_FOR_SIZE_CHECK;

    repetitionLevelColumn = getColumnDescriptorValuesWriter(path.getMaxRepetitionLevel());
    definitionLevelColumn = getColumnDescriptorValuesWriter(path.getMaxDefinitionLevel());

    if (enableDictionary) {
      switch (path.getType()) {
      case BOOLEAN:
        this.dataColumn = new BooleanPlainValuesWriter();
        break;
      case BINARY:
        this.dataColumn = new PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
        break;
      case INT64:
        this.dataColumn = new PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
        break;
      case DOUBLE:
        this.dataColumn = new PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
        break;
      case INT32:
        this.dataColumn = new PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
        break;
      case FLOAT:
        this.dataColumn = new PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol);
        break;
      case FIXED_LEN_BYTE_ARRAY:
        this.dataColumn = new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol);
        break;
      default:
        this.dataColumn = new PlainValuesWriter(initialSizePerCol);
      }
    } else {
      switch (path.getType()) {
      case BOOLEAN:
        this.dataColumn = new BooleanPlainValuesWriter();
        break;
      case FIXED_LEN_BYTE_ARRAY:
        this.dataColumn = new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol);
        break;
      default:
        this.dataColumn = new PlainValuesWriter(initialSizePerCol);
      }
    }
  }

  private ValuesWriter getColumnDescriptorValuesWriter(int maxLevel) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      // TODO: what is a good initialCapacity?
      return new RunLengthBitPackingHybridValuesWriter(
          BytesUtils.getWidthFromMaxInt(maxLevel),
          64 * 1024);
    }
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path + " " + value + " r:" + r + " d:" + d);
  }

  /**
   * Counts how many values have been written and checks the memory usage to flush the page when we reach the page threshold.
   *
   * We measure the memory used when we reach the mid point toward our estimated count.
   * We then update the estimate and flush the page if we reached the threshold.
   *
   * That way we check the memory size log2(n) times.
   *
   */
  private void accountForValueWritten() {
    ++ valueCount;
    if (valueCount > valueCountForNextSizeCheck) {
      // not checking the memory used for every value
      long memSize = repetitionLevelColumn.getBufferedSize()
          + definitionLevelColumn.getBufferedSize()
          + dataColumn.getBufferedSize();
      if (memSize > pageSizeThreshold) {
        // we will write the current page and check again the size at the predicted middle of next page
        valueCountForNextSizeCheck = valueCount / 2;
        writePage();
      } else {
        // not reached the threshold, will check again midway
        valueCountForNextSizeCheck = (int)(valueCount + ((float)valueCount * pageSizeThreshold / memSize)) / 2 + 1;
      }
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
    final DictionaryPage dictionaryPage = dataColumn.createDictionaryPage();
    if (dictionaryPage != null) {
      if (DEBUG) LOG.debug("write dictionary");
      try {
        pageWriter.writeDictionaryPage(dictionaryPage);
      } catch (IOException e) {
        throw new ParquetEncodingException("could not write dictionary page for " + path, e);
      }
      dataColumn.resetDictionary();
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

  public String memUsageString(String indent) {
    StringBuilder b = new StringBuilder(indent).append(path).append(" {\n");
    b.append(repetitionLevelColumn.memUsageString(indent + "  r:")).append("\n");
    b.append(definitionLevelColumn.memUsageString(indent + "  d:")).append("\n");
    b.append(dataColumn.memUsageString(indent + "  data:")).append("\n");
    b.append(pageWriter.memUsageString(indent + "  pages:")).append("\n");
    b.append(indent).append(String.format("  total: %,d/%,d", getBufferedSizeInMemory(), allocatedSize())).append("\n");
    b.append(indent).append("}\n");
    return b.toString();
  }
}
