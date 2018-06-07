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
package org.apache.parquet.hadoop.metadata;

import java.io.IOException;
import java.util.Set;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 */
abstract public class ColumnChunkMetaData {
  
  // Hidden is an encrypted column for which the reader doesn't have a key
  protected boolean hiddenColumn;
  protected ColumnPath path;

  @Deprecated
  public static ColumnChunkMetaData get(
      ColumnPath path,
      PrimitiveTypeName type,
      CompressionCodecName codec,
      Set<Encoding> encodings,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    return get(
        path, type, codec, null, encodings, new BooleanStatistics(), firstDataPage,
        dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
  }

  @Deprecated
  public static ColumnChunkMetaData get(
      ColumnPath path,
      PrimitiveTypeName type,
      CompressionCodecName codec,
      Set<Encoding> encodings,
      Statistics statistics,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    return get(
        path, type, codec, null, encodings, statistics, firstDataPage, dictionaryPageOffset,
        valueCount, totalSize, totalUncompressedSize);
  }

  /**
   * @param path the path of this column in the write schema
   * @param type primitive type for this column
   * @param codec the compression codec used to compress
   * @param encodingStats EncodingStats for the encodings used in this column
   * @param encodings a set of encoding used in this column
   * @param statistics statistics for the data in this column
   * @param firstDataPage offset of the first non-dictionary page
   * @param dictionaryPageOffset offset of the the dictionary page
   * @param valueCount number of values
   * @param totalSize total compressed size
   * @param totalUncompressedSize uncompressed data size
   * @return a column chunk metadata instance
   * @deprecated will be removed in 2.0.0. Use
   *             {@link #get(ColumnPath, PrimitiveType, CompressionCodecName, EncodingStats, Set, Statistics, long, long, long, long, long)}
   *             instead.
   */
  @Deprecated
  public static ColumnChunkMetaData get(
      ColumnPath path,
      PrimitiveTypeName type,
      CompressionCodecName codec,
      EncodingStats encodingStats,
      Set<Encoding> encodings,
      Statistics statistics,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    return get(path, Types.optional(type).named("fake_type"), codec, encodingStats, encodings, statistics,
        firstDataPage, dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
  }

  public static ColumnChunkMetaData get(
      ColumnPath path,
      PrimitiveType type,
      CompressionCodecName codec,
      EncodingStats encodingStats,
      Set<Encoding> encodings,
      Statistics statistics,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    // to save space we store those always positive longs in ints when they fit.
    if (positiveLongFitsInAnInt(firstDataPage)
        && positiveLongFitsInAnInt(dictionaryPageOffset)
        && positiveLongFitsInAnInt(valueCount)
        && positiveLongFitsInAnInt(totalSize)
        && positiveLongFitsInAnInt(totalUncompressedSize)) {
      return new IntColumnChunkMetaData(
          path, type, codec,
          encodingStats, encodings,
          statistics,
          firstDataPage,
          dictionaryPageOffset,
          valueCount,
          totalSize,
          totalUncompressedSize);
    } else {
      return new LongColumnChunkMetaData(
          path, type, codec,
          encodingStats, encodings,
          statistics,
          firstDataPage,
          dictionaryPageOffset,
          valueCount,
          totalSize,
          totalUncompressedSize);
    }
  }
  
  public static ColumnChunkMetaData getHiddenColumn(ColumnPath path) {
    return new HiddenColumnChunkMetaData(path);
  }

  /**
   * @return the offset of the first byte in the chunk
   */
  public long getStartingPos() { // TODO change method signature - add throws IOException
    if (hiddenColumn) throw new RuntimeException("Hidden column"); // TODO replace with IOException
    long dictionaryPageOffset = getDictionaryPageOffset();
    long firstDataPageOffset = getFirstDataPageOffset();
    if (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
      // if there's a dictionary and it's before the first data page, start from there
      return dictionaryPageOffset;
    }
    return firstDataPageOffset;
  }
  
  public boolean isHiddenColumn() {
    return hiddenColumn;
  }

  /**
   * checks that a positive long value fits in an int.
   * (reindexed on Integer.MIN_VALUE)
   * @param value a long value
   * @return whether it fits
   */
  protected static boolean positiveLongFitsInAnInt(long value) {
    return (value >= 0) && (value + Integer.MIN_VALUE <= Integer.MAX_VALUE);
  }

  private final EncodingStats encodingStats;

  // we save 3 references by storing together the column properties that have few distinct values
  private final ColumnChunkProperties properties;

  protected ColumnChunkMetaData(ColumnChunkProperties columnChunkProperties) {
    this(null, columnChunkProperties);
  }

  protected ColumnChunkMetaData(EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties) {
    this.encodingStats = encodingStats;
    this.properties = columnChunkProperties;
  }

  public CompressionCodecName getCodec() { // TODO change method signature - add throws IOException
    if (hiddenColumn) throw new RuntimeException("Hidden column"); // TODO replace with IOException
    return properties.getCodec();
  }

  /**
   *
   * @return column identifier
   * @deprecated will be removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   */
  @Deprecated
  public ColumnPath getPath() {
    if (hiddenColumn) return path;
    return properties.getPath();
  }

  /**
   * @return type of the column
   * @deprecated will be removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   */
  @Deprecated
  public PrimitiveTypeName getType() {
    return properties.getType();
  }

  /**
   * @return the primitive type object of the column
   */
  public PrimitiveType getPrimitiveType() { // TODO change method signature - add throws IOException
    if (hiddenColumn) throw new RuntimeException("Hidden column"); // TODO replace with IOException
    return properties.getPrimitiveType();
  }

  /**
   * @return start of the column data offset
   */
  abstract public long getFirstDataPageOffset(); // TODO change method signature - add throws IOException

  /**
   * @return the location of the dictionary page if any
   */
  abstract public long getDictionaryPageOffset(); // TODO change method signature - add throws IOException

  /**
   * @return count of values in this block of the column
   */
  abstract public long getValueCount(); // TODO change method signature - add throws IOException

  /**
   * @return the totalUncompressedSize
   */
  abstract public long getTotalUncompressedSize(); // TODO change method signature - add throws IOException

  /**
   * @return the totalSize
   */
  abstract public long getTotalSize(); // TODO change method signature - add throws IOException

  /**
   * @return the stats for this column
   */
  abstract public Statistics getStatistics(); // TODO change method signature - add throws IOException

  /**
   * @return all the encodings used in this column
   */
  public Set<Encoding> getEncodings() { // TODO change method signature - add throws IOException
    if (hiddenColumn) throw new RuntimeException("Hidden column"); // TODO replace with IOException
    return properties.getEncodings();
  }

  public EncodingStats getEncodingStats() { // TODO change method signature - add throws IOException
    if (hiddenColumn) throw new RuntimeException("Hidden column"); // TODO replace with IOException
    return encodingStats;
  }

  @Override
  public String toString() {
    if (hiddenColumn) return "ColumnMetaData{" + path.toString() +" - Hidden column}";
    return "ColumnMetaData{" + properties.toString() + ", " + getFirstDataPageOffset() + "}";
  }
}

class IntColumnChunkMetaData extends ColumnChunkMetaData {

  private final int firstDataPage;
  private final int dictionaryPageOffset;
  private final int valueCount;
  private final int totalSize;
  private final int totalUncompressedSize;
  private final Statistics statistics;

  /**
   * @param path column identifier
   * @param type type of the column
   * @param codec
   * @param encodings
   * @param statistics
   * @param firstDataPage
   * @param dictionaryPageOffset
   * @param valueCount
   * @param totalSize
   * @param totalUncompressedSize
   */
  IntColumnChunkMetaData(
      ColumnPath path,
      PrimitiveType type,
      CompressionCodecName codec,
      EncodingStats encodingStats,
      Set<Encoding> encodings,
      Statistics statistics,
      long firstDataPage,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPage = positiveLongToInt(firstDataPage);
    this.dictionaryPageOffset = positiveLongToInt(dictionaryPageOffset);
    this.valueCount = positiveLongToInt(valueCount);
    this.totalSize = positiveLongToInt(totalSize);
    this.totalUncompressedSize = positiveLongToInt(totalUncompressedSize);
    this.statistics = statistics;
    this.hiddenColumn = false;
  }

  /**
   * stores a positive long into an int (assuming it fits)
   * @param value
   * @return
   */
  private int positiveLongToInt(long value) {
    if (!ColumnChunkMetaData.positiveLongFitsInAnInt(value)) {
      throw new IllegalArgumentException("value should be positive and fit in an int: " + value);
    }
    return (int)(value + Integer.MIN_VALUE);
  }

  /**
   * turns the int back into a positive long
   * @param value
   * @return
   */
  private long intToPositiveLong(int value) {
    return (long)value - Integer.MIN_VALUE;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return intToPositiveLong(firstDataPage);
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return intToPositiveLong(dictionaryPageOffset);
  }

  /**
   * @return count of values in this block of the column
   */
  public long getValueCount() {
    return intToPositiveLong(valueCount);
  }

  /**
   * @return the totalUncompressedSize
   */
  public long getTotalUncompressedSize() {
    return intToPositiveLong(totalUncompressedSize);
  }

  /**
   * @return the totalSize
   */
  public long getTotalSize() {
    return intToPositiveLong(totalSize);
  }

  /**
   * @return the stats for this column
   */
  public Statistics getStatistics() {
   return statistics;
  }
}

class LongColumnChunkMetaData extends ColumnChunkMetaData {

  private final long firstDataPageOffset;
  private final long dictionaryPageOffset;
  private final long valueCount;
  private final long totalSize;
  private final long totalUncompressedSize;
  private final Statistics statistics;

  /**
   * @param path column identifier
   * @param type type of the column
   * @param codec
   * @param encodings
   * @param statistics
   * @param firstDataPageOffset
   * @param dictionaryPageOffset
   * @param valueCount
   * @param totalSize
   * @param totalUncompressedSize
   */
  LongColumnChunkMetaData(
      ColumnPath path,
      PrimitiveType type,
      CompressionCodecName codec,
      EncodingStats encodingStats,
      Set<Encoding> encodings,
      Statistics statistics,
      long firstDataPageOffset,
      long dictionaryPageOffset,
      long valueCount,
      long totalSize,
      long totalUncompressedSize) {
    super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPageOffset = firstDataPageOffset;
    this.dictionaryPageOffset = dictionaryPageOffset;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
    this.totalUncompressedSize = totalUncompressedSize;
    this.statistics = statistics;
    this.hiddenColumn = false;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPageOffset;
  }

  /**
   * @return the location of the dictionary page if any
   */
  public long getDictionaryPageOffset() {
    return dictionaryPageOffset;
  }

  /**
   * @return count of values in this block of the column
   */
  public long getValueCount() {
    return valueCount;
  }

  /**
   * @return the totalUncompressedSize
   */
  public long getTotalUncompressedSize() {
    return totalUncompressedSize;
  }

  /**
   * @return the totalSize
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * @return the stats for this column
   */
  public Statistics getStatistics() {
   return statistics;
  }
}

class HiddenColumnChunkMetaData extends ColumnChunkMetaData {
  HiddenColumnChunkMetaData(ColumnPath path) {
    super((EncodingStats) null, (ColumnChunkProperties) null);
    this.path = path;
    this.hiddenColumn = true;
  }

  @Override
  public long getFirstDataPageOffset() {
    throw new RuntimeException("Hidden column"); // TODO replace with throws IOException
  }

  @Override
  public long getDictionaryPageOffset() {
    throw new RuntimeException("Hidden column"); // TODO replace with IOException
  }

  @Override
  public long getValueCount() {
    throw new RuntimeException("Hidden column"); // TODO replace with IOException
  }

  @Override
  public long getTotalUncompressedSize() {
    throw new RuntimeException("Hidden column"); // TODO replace with IOException
  }

  @Override
  public long getTotalSize() {
    throw new RuntimeException("Hidden column"); // TODO replace with IOException
  }

  @Override
  public Statistics getStatistics() {
    throw new RuntimeException("Hidden column"); // TODO replace with IOException
  }
}

