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

import java.io.IOException;
import java.util.HashMap;

import org.apache.parquet.Ints;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 */
final class ColumnWriterV2 implements ColumnWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnWriterV2.class);

  // By default: Debugging disabled this way (using the "if (DEBUG)" IN the methods) to allow
  // the java compiler (not the JIT) to remove the unused statements during build time.
  private static final boolean DEBUG = false;

  private final ColumnDescriptor path;
  private final PageWriter pageWriter;
  private RunLengthBitPackingHybridEncoder repetitionLevelColumn;
  private RunLengthBitPackingHybridEncoder definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;

  private BloomFilterWriter bloomFilterWriter;
  private BloomFilter bloomFilter;

  private Statistics<?> statistics;
  private long rowsWrittenSoFar = 0;

  public ColumnWriterV2(
      ColumnDescriptor path,
      PageWriter pageWriter,
      ParquetProperties props) {
    this.path = path;
    this.pageWriter = pageWriter;
    resetStatistics();

    this.repetitionLevelColumn = props.newRepetitionLevelEncoder(path);
    this.definitionLevelColumn = props.newDefinitionLevelEncoder(path);
    this.dataColumn = props.newValuesWriter(path);
  }

  public ColumnWriterV2(
    ColumnDescriptor path,
    PageWriter pageWriter,
    BloomFilterWriter bloomFilterWriter,
    ParquetProperties props) {
    this(path, pageWriter, props);

    this.bloomFilterWriter = bloomFilterWriter;
    HashMap<String, Long> bloomFilterInfo = props.getBloomFilterInfo();

    // Current not support nested column.
    if (path.getPath().length == 1) {
      String column = path.getPath()[0];
      if (bloomFilterInfo.keySet().contains(column)) {
        this.bloomFilter = new BloomFilter(bloomFilterInfo.get(column).intValue());
      }
    }
  }

  private void log(Object value, int r, int d) {
    LOG.debug("{} {} r:{} d:{}", path, value, r, d);
  }

  private void resetStatistics() {
    this.statistics = Statistics.createStats(path.getPrimitiveType());
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

  @Override
  public void close() {
    // Close the Values writers.
    repetitionLevelColumn.close();
    definitionLevelColumn.close();
    dataColumn.close();
  }

  @Override
  public long getBufferedSizeInMemory() {
    return repetitionLevelColumn.getBufferedSize()
      + definitionLevelColumn.getBufferedSize()
      + dataColumn.getBufferedSize()
      + pageWriter.getMemSize();
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
    if (bloomFilter != null) {
      bloomFilter.insert(bloomFilter.hash(value));
    }
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
    if (bloomFilter != null) {
      bloomFilter.insert(bloomFilter.hash(value));
    }
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
    if (bloomFilter != null) {
      bloomFilter.insert(bloomFilter.hash(value));
    }
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
    if (bloomFilter != null) {
      bloomFilter.insert(bloomFilter.hash(value));
    }
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
    if (bloomFilter != null) {
      bloomFilter.insert(bloomFilter.hash(value));
    }
    ++ valueCount;
  }

  /**
   * Finalizes the Column chunk. Possibly adding extra pages if needed (dictionary, ...)
   * Is called right after writePage
   */
  public void finalizeColumnChunk() {
    final DictionaryPage dictionaryPage = dataColumn.toDictPageAndClose();
    if (dictionaryPage != null) {
      if (DEBUG) LOG.debug("write dictionary");
      try {
        pageWriter.writeDictionaryPage(dictionaryPage);
      } catch (IOException e) {
        throw new ParquetEncodingException("could not write dictionary page for " + path, e);
      }
      dataColumn.resetDictionary();
    }

    if (bloomFilterWriter != null && bloomFilter != null) {
      bloomFilterWriter.writeBloomFilter(bloomFilter);
    }
  }

  /**
   * used to decide when to write a page
   * @return the number of bytes of memory used to buffer the current data
   */
  public long getCurrentPageBufferedSize() {
    long bloomBufferSize = bloomFilter == null ? 0 : bloomFilter.getBufferedSize();
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + bloomBufferSize;
  }

  /**
   * used to decide when to write a page or row group
   * @return the number of bytes of memory used to buffer the current data and the previously written pages
   */
  public long getTotalBufferedSize() {
    long bloomBufferSize = bloomFilter == null ? 0 : bloomFilter.getBufferedSize();
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + pageWriter.getMemSize()
        + bloomBufferSize;
  }

  /**
   * @return actual memory used
   */
  public long allocatedSize() {
    long bloomFilterSize = bloomFilter == null ? 0 : bloomFilter.getBufferedSize();
    return repetitionLevelColumn.getAllocatedSize()
    + definitionLevelColumn.getAllocatedSize()
    + dataColumn.getAllocatedSize()
    + pageWriter.allocatedSize()
    + bloomFilterSize;
  }

  /**
   * @param indent a prefix to format lines
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
    int pageRowCount = Ints.checkedCast(rowCount - rowsWrittenSoFar);
    this.rowsWrittenSoFar = rowCount;
    if (DEBUG) LOG.debug("write page");
    try {
      // TODO: rework this API. Those must be called *in that order*
      BytesInput bytes = dataColumn.getBytes();
      Encoding encoding = dataColumn.getEncoding();
      pageWriter.writePageV2(
          pageRowCount,
          Ints.checkedCast(statistics.getNumNulls()),
          valueCount,
          path.getMaxRepetitionLevel() == 0 ? BytesInput.empty() : repetitionLevelColumn.toBytes(),
          path.getMaxDefinitionLevel() == 0 ? BytesInput.empty() : definitionLevelColumn.toBytes(),
          encoding,
          bytes,
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
