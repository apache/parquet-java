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

import java.util.Set;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.yetus.audience.InterfaceAudience.Private;

/**
 * Column meta data for a block stored in the file footer and passed in the
 * InputSplit
 */
abstract public class ColumnChunkMetaData {

  @Deprecated
  public static ColumnChunkMetaData get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec,
      Set<Encoding> encodings, long firstDataPage, long dictionaryPageOffset, long valueCount, long totalSize,
      long totalUncompressedSize) {
    return get(path, type, codec, null, encodings, new BooleanStatistics(), firstDataPage, dictionaryPageOffset,
        valueCount, totalSize, totalUncompressedSize);
  }

  @Deprecated
  public static ColumnChunkMetaData get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec,
      Set<Encoding> encodings, Statistics statistics, long firstDataPage, long dictionaryPageOffset, long valueCount,
      long totalSize, long totalUncompressedSize) {
    return get(path, type, codec, null, encodings, statistics, firstDataPage, dictionaryPageOffset, valueCount,
        totalSize, totalUncompressedSize);
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
   * {@link #get(ColumnPath, PrimitiveType, CompressionCodecName, EncodingStats, Set, Statistics, long, long, long, long, long)}
   * instead.
   */
  @Deprecated
  public static ColumnChunkMetaData get(ColumnPath path, PrimitiveTypeName type, CompressionCodecName codec,
      EncodingStats encodingStats, Set<Encoding> encodings, Statistics statistics, long firstDataPage,
      long dictionaryPageOffset, long valueCount, long totalSize, long totalUncompressedSize) {
    return get(path, Types.optional(type).named("fake_type"), codec, encodingStats, encodings, statistics,
        firstDataPage, dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
  }

  public static ColumnChunkMetaData get(ColumnPath path, PrimitiveType type, CompressionCodecName codec,
      EncodingStats encodingStats, Set<Encoding> encodings, Statistics statistics, long firstDataPage,
      long dictionaryPageOffset, long valueCount, long totalSize, long totalUncompressedSize) {
    // to save space we store those always positive longs in ints when they fit.
    if (positiveLongFitsInAnInt(firstDataPage) && positiveLongFitsInAnInt(dictionaryPageOffset)
        && positiveLongFitsInAnInt(valueCount) && positiveLongFitsInAnInt(totalSize)
        && positiveLongFitsInAnInt(totalUncompressedSize)) {
      return new IntColumnChunkMetaData(path, type, codec, encodingStats, encodings, statistics, firstDataPage,
          dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
    } else {
      return new LongColumnChunkMetaData(path, type, codec, encodingStats, encodings, statistics, firstDataPage,
          dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize);
    }
  }

  /**
   * @return the offset of the first byte in the chunk
   */
  public long getStartingPos() {
    long dictionaryPageOffset = getDictionaryPageOffset();
    long firstDataPageOffset = getFirstDataPageOffset();
    if (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
      // if there's a dictionary and it's before the first data page, start from there
      return dictionaryPageOffset;
    }
    return firstDataPageOffset;
  }

  /**
   * checks that a positive long value fits in an int. (reindexed on
   * Integer.MIN_VALUE)
   * 
   * @param value a long value
   * @return whether it fits
   */
  protected static boolean positiveLongFitsInAnInt(long value) {
    return (value >= 0) && (value + Integer.MIN_VALUE <= Integer.MAX_VALUE);
  }

  private final EncodingStats encodingStats;

  // we save 3 references by storing together the column properties that have few
  // distinct values
  private final ColumnChunkProperties properties;

  private IndexReference columnIndexReference;
  private IndexReference offsetIndexReference;

  protected ColumnChunkMetaData(ColumnChunkProperties columnChunkProperties) {
    this(null, columnChunkProperties);
  }

  protected ColumnChunkMetaData(EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties) {
    this.encodingStats = encodingStats;
    this.properties = columnChunkProperties;
  }

  public CompressionCodecName getCodec() {
    return properties.getCodec();
  }

  /**
   *
   * @return column identifier
   */
  public ColumnPath getPath() {
    return properties.getPath();
  }

  /**
   * @return type of the column
   * @deprecated will be removed in 2.0.0. Use {@link #getPrimitiveType()}
   * instead.
   */
  @Deprecated
  public PrimitiveTypeName getType() {
    return properties.getType();
  }

  /**
   * @return the primitive type object of the column
   */
  public PrimitiveType getPrimitiveType() {
    return properties.getPrimitiveType();
  }

  /**
   * @return start of the column data offset
   */
  abstract public long getFirstDataPageOffset();

  /**
   * @return the location of the dictionary page if any
   */
  abstract public long getDictionaryPageOffset();

  /**
   * @return count of values in this block of the column
   */
  abstract public long getValueCount();

  /**
   * @return the totalUncompressedSize
   */
  abstract public long getTotalUncompressedSize();

  /**
   * @return the totalSize
   */
  abstract public long getTotalSize();

  /**
   * @return the stats for this column
   */
  abstract public Statistics getStatistics();

  /**
   * @return the reference to the column index
   */
  @Private
  public IndexReference getColumnIndexReference() {
    return columnIndexReference;
  }

  /**
   * @param indexReference the reference to the column index
   */
  @Private
  public void setColumnIndexReference(IndexReference indexReference) {
    this.columnIndexReference = indexReference;
  }

  /**
   * @return the reference to the offset index
   */
  @Private
  public IndexReference getOffsetIndexReference() {
    return offsetIndexReference;
  }

  /**
   * @param offsetIndexReference the reference to the offset index
   */
  @Private
  public void setOffsetIndexReference(IndexReference offsetIndexReference) {
    this.offsetIndexReference = offsetIndexReference;
  }

  /**
   * @return all the encodings used in this column
   */
  public Set<Encoding> getEncodings() {
    return properties.getEncodings();
  }

  public EncodingStats getEncodingStats() {
    return encodingStats;
  }

  @Override
  public String toString() {
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
  IntColumnChunkMetaData(ColumnPath path, PrimitiveType type, CompressionCodecName codec, EncodingStats encodingStats,
      Set<Encoding> encodings, Statistics statistics, long firstDataPage, long dictionaryPageOffset, long valueCount,
      long totalSize, long totalUncompressedSize) {
    super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPage = positiveLongToInt(firstDataPage);
    this.dictionaryPageOffset = positiveLongToInt(dictionaryPageOffset);
    this.valueCount = positiveLongToInt(valueCount);
    this.totalSize = positiveLongToInt(totalSize);
    this.totalUncompressedSize = positiveLongToInt(totalUncompressedSize);
    this.statistics = statistics;
  }

  /**
   * stores a positive long into an int (assuming it fits)
   * 
   * @param value
   * @return
   */
  private int positiveLongToInt(long value) {
    if (!ColumnChunkMetaData.positiveLongFitsInAnInt(value)) {
      throw new IllegalArgumentException("value should be positive and fit in an int: " + value);
    }
    return (int) (value + Integer.MIN_VALUE);
  }

  /**
   * turns the int back into a positive long
   * 
   * @param value
   * @return
   */
  private long intToPositiveLong(int value) {
    return (long) value - Integer.MIN_VALUE;
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
  LongColumnChunkMetaData(ColumnPath path, PrimitiveType type, CompressionCodecName codec, EncodingStats encodingStats,
      Set<Encoding> encodings, Statistics statistics, long firstDataPageOffset, long dictionaryPageOffset,
      long valueCount, long totalSize, long totalUncompressedSize) {
    super(encodingStats, ColumnChunkProperties.get(path, type, codec, encodings));
    this.firstDataPageOffset = firstDataPageOffset;
    this.dictionaryPageOffset = dictionaryPageOffset;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
    this.totalUncompressedSize = totalUncompressedSize;
    this.statistics = statistics;
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
