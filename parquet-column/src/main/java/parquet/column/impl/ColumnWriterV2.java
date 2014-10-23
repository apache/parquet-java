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

import java.io.IOException;

import parquet.Ints;
import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnWriter;
import parquet.column.ParquetProperties;
import parquet.column.impl.ColumnWriterV2;
import parquet.column.page.DictionaryPage;
import parquet.column.page.PageWriter;
import parquet.column.statistics.Statistics;
import parquet.column.values.ValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 *
 * @author Julien Le Dem
 *
 */
final class ColumnWriterV2 implements ColumnWriter {
  private static final Log LOG = Log.getLog(ColumnWriterV2.class);
  private static final boolean DEBUG = Log.DEBUG;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private RunLengthBitPackingHybridEncoder repetitionLevelColumn;
  private RunLengthBitPackingHybridEncoder definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;

  private Statistics<?> statistics;
  private long rowsWrittenSoFar = 0;

  public ColumnWriterV2(
      ColumnDescriptor path,
      PageWriter pageWriter,
      int initialSizePerCol,
      ParquetProperties parquetProps) {
    this.path = path;
    this.pageWriter = pageWriter;
    resetStatistics();
    this.repetitionLevelColumn = new RunLengthBitPackingHybridEncoder(path.getMaxRepetitionLevel(), initialSizePerCol);
    this.definitionLevelColumn = new RunLengthBitPackingHybridEncoder(path.getMaxDefinitionLevel(), initialSizePerCol);
    this.dataColumn = parquetProps.getValuesWriter(path, initialSizePerCol);
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path + " " + value + " r:" + r + " d:" + d);
  }

  private void resetStatistics() {
    this.statistics = Statistics.getStatsBasedOnType(this.path.getType());
  }

  private void definitionLevel(int definitionLevel) {
    try {
      definitionLevelColumn.writeInt(definitionLevel);
    } catch (IOException e) {
      throw new ParquetEncodingException("illegal definition level " + definitionLevel + " for column " + path, e);
    }
  }

  private void repetitionLevel(int repetitionLevel) {
    try {
      repetitionLevelColumn.writeInt(repetitionLevel);
    } catch (IOException e) {
      throw new ParquetEncodingException("illegal repetition level " + repetitionLevel + " for column " + path, e);
    }
  }

  /**
   * writes the current null value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    statistics.incrementNumNulls();
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeDouble(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeFloat(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeBytes(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeBoolean(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeInteger(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  public void write(long value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeLong(value);
    statistics.updateStats(value);
    ++ valueCount;
  }

  /**
   * Finalizes the Column chunk. Possibly adding extra pages if needed (dictionary, ...)
   * Is called right after writePage
   */
  public void finalizeColumnChunk() {
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

  /**
   * used to decide when to write a page
   * @return the number of bytes of memory used to buffer the current data
   */
  public long getCurrentPageBufferedSize() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize();
  }

  /**
   * used to decide when to write a page or row group
   * @return the number of bytes of memory used to buffer the current data and the previously written pages
   */
  public long getTotalBufferedSize() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + pageWriter.getMemSize();
  }

  /**
   * @return actual memory used
   */
  public long allocatedSize() {
    return repetitionLevelColumn.getAllocatedSize()
    + definitionLevelColumn.getAllocatedSize()
    + dataColumn.getAllocatedSize()
    + pageWriter.allocatedSize();
  }

  /**
   * @param prefix a prefix to format lines
   * @return a formatted string showing how memory is used
   */
  public String memUsageString(String indent) {
    StringBuilder b = new StringBuilder(indent).append(path).append(" {\n");
    b.append(indent).append(" r:").append(repetitionLevelColumn.getAllocatedSize()).append(" bytes\n");
    b.append(indent).append(" d:").append(definitionLevelColumn.getAllocatedSize()).append(" bytes\n");
    b.append(dataColumn.memUsageString(indent + "  data:")).append("\n");
    b.append(pageWriter.memUsageString(indent + "  pages:")).append("\n");
    b.append(indent).append(String.format("  total: %,d/%,d", getTotalBufferedSize(), allocatedSize())).append("\n");
    b.append(indent).append("}\n");
    return b.toString();
  }

  public long getRowsWrittenSoFar() {
    return this.rowsWrittenSoFar;
  }

  /**
   * writes the current data to a new page in the page store
   * @param rowCount how many rows have been written so far
   */
  public void writePage(long rowCount) {
    int pageRowCount = Ints.checkedCast(rowsWrittenSoFar - rowCount);
    this.rowsWrittenSoFar = rowCount;
    if (DEBUG) LOG.debug("write page");
    try {
      pageWriter.writePageV2(
          pageRowCount,
          Ints.checkedCast(statistics.getNumNulls()),
          valueCount,
          repetitionLevelColumn.toBytes(),
          definitionLevelColumn.toBytes(),
          dataColumn.getEncoding(),
          dataColumn.getBytes(),
          statistics
          );
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page for " + path, e);
    }
    repetitionLevelColumn.reset();
    definitionLevelColumn.reset();
    dataColumn.reset();
    valueCount = 0;
    resetStatistics();
  }
}
