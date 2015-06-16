/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.impl;

import static org.apache.parquet.bytes.BytesInput.concat;

import java.io.IOException;

import org.apache.parquet.Log;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;

import static java.lang.Math.max;

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 *
 * @author Julien Le Dem
 *
 */
final class ColumnWriterV1 implements ColumnWriter {
  private static final Log LOG = Log.getLog(ColumnWriterV1.class);
  private static final boolean DEBUG = Log.DEBUG;
  private static final int INITIAL_COUNT_FOR_SIZE_CHECK = 100;
  private static final int MIN_SLAB_SIZE = 64;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private final long pageSizeThreshold;
  private ValuesWriter repetitionLevelColumn;
  private ValuesWriter definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;
  private int valueCountForNextSizeCheck;

  private Statistics statistics;

  public ColumnWriterV1(
      ColumnDescriptor path,
      PageWriter pageWriter,
      int pageSizeThreshold,
      int dictionaryPageSizeThreshold,
      boolean enableDictionary,
      WriterVersion writerVersion) {
    this.path = path;
    this.pageWriter = pageWriter;
    this.pageSizeThreshold = pageSizeThreshold;
    // initial check of memory usage. So that we have enough data to make an initial prediction
    this.valueCountForNextSizeCheck = INITIAL_COUNT_FOR_SIZE_CHECK;
    resetStatistics();

    ParquetProperties parquetProps = new ParquetProperties(dictionaryPageSizeThreshold, writerVersion, enableDictionary);

    this.repetitionLevelColumn = ParquetProperties.getColumnDescriptorValuesWriter(path.getMaxRepetitionLevel(), MIN_SLAB_SIZE, pageSizeThreshold);
    this.definitionLevelColumn = ParquetProperties.getColumnDescriptorValuesWriter(path.getMaxDefinitionLevel(), MIN_SLAB_SIZE, pageSizeThreshold);

    int initialSlabSize = CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_SLAB_SIZE, pageSizeThreshold, 10);
    this.dataColumn = parquetProps.getValuesWriter(path, initialSlabSize, pageSizeThreshold);
  }

  private void log(Object value, int r, int d) {
    LOG.debug(path + " " + value + " r:" + r + " d:" + d);
  }

  private void resetStatistics() {
    this.statistics = Statistics.getStatsBasedOnType(this.path.getType());
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

  private void updateStatisticsNumNulls() {
    statistics.incrementNumNulls();
  }

  private void updateStatistics(int value) {
    statistics.updateStats(value);
  }

  private void updateStatistics(long value) {
    statistics.updateStats(value);
  }

  private void updateStatistics(float value) {
    statistics.updateStats(value);
  }

  private void updateStatistics(double value) {
   statistics.updateStats(value);
  }

  private void updateStatistics(Binary value) {
   statistics.updateStats(value);
  }

  private void updateStatistics(boolean value) {
   statistics.updateStats(value);
  }

  private void writePage() {
    if (DEBUG) LOG.debug("write page");
    try {
      pageWriter.writePage(
          concat(repetitionLevelColumn.getBytes(), definitionLevelColumn.getBytes(), dataColumn.getBytes()),
          valueCount,
          statistics,
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
    resetStatistics();
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(null, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    updateStatisticsNumNulls();
    accountForValueWritten();
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeDouble(value);
    updateStatistics(value);
    accountForValueWritten();
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeFloat(value);
    updateStatistics(value);
    accountForValueWritten();
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeBytes(value);
    updateStatistics(value);
    accountForValueWritten();
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeBoolean(value);
    updateStatistics(value);
    accountForValueWritten();
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeInteger(value);
    updateStatistics(value);
    accountForValueWritten();
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    if (DEBUG) log(value, repetitionLevel, definitionLevel);
    repetitionLevelColumn.writeInteger(repetitionLevel);
    definitionLevelColumn.writeInteger(definitionLevel);
    dataColumn.writeLong(value);
    updateStatistics(value);
    accountForValueWritten();
  }

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
