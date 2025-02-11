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

import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 */
public abstract class ColumnChunkMetaData {
  protected int rowGroupOrdinal = -1;

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
        path,
        type,
        codec,
        null,
        encodings,
        new BooleanStatistics(),
        firstDataPage,
        dictionaryPageOffset,
        valueCount,
        totalSize,
        totalUncompressedSize);
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
        path,
        type,
        codec,
        null,
        encodings,
        statistics,
        firstDataPage,
        dictionaryPageOffset,
        valueCount,
        totalSize,
        totalUncompressedSize);
  }

  /**
   * @param path                  the path of this column in the write schema
   * @param type                  primitive type for this column
   * @param codec                 the compression codec used to compress
   * @param encodingStats         EncodingStats for the encodings used in this column
   * @param encodings             a set of encoding used in this column
   * @param statistics            statistics for the data in this column
   * @param firstDataPage         offset of the first non-dictionary page
   * @param dictionaryPageOffset  offset of the the dictionary page
   * @param valueCount            number of values
   * @param totalSize             total compressed size
   * @param totalUncompressedSize uncompressed data size
   * @return a column chunk metadata instance
   * @deprecated will be removed in 2.0.0. Use
   * {@link #get(ColumnPath, PrimitiveType, CompressionCodecName, EncodingStats, Set, Statistics, long, long, long, long, long)}
   * instead.
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

    return get(
        path,
        Types.optional(type).named("fake_type"),
        codec,
        encodingStats,
        encodings,
        statistics,
        firstDataPage,
        dictionaryPageOffset,
        valueCount,
        totalSize,
        totalUncompressedSize);
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
    return get(
        path,
        type,
        codec,
        encodingStats,
        encodings,
        statistics,
        firstDataPage,
        dictionaryPageOffset,
        valueCount,
        totalSize,
        totalUncompressedSize,
        null);
  }

  /**
   * @param path                  the path of this column in the write schema
   * @param type                  primitive type for this column
   * @param codec                 the compression codec used to compress
   * @param encodingStats         EncodingStats for the encodings used in this column
   * @param encodings             a set of encoding used in this column
   * @param statistics            statistics for the data in this column
   * @param firstDataPage         offset of the first non-dictionary page
   * @param dictionaryPageOffset  offset of the dictionary page
   * @param valueCount            number of values
   * @param totalSize             total compressed size
   * @param totalUncompressedSize uncompressed data size
   * @param sizeStatistics        size statistics for the data in this column
   * @return a column chunk metadata instance
   */
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
      long totalUncompressedSize,
      SizeStatistics sizeStatistics) {

    // to save space we store those always positive longs in ints when they fit.
    if (positiveLongFitsInAnInt(firstDataPage)
        && positiveLongFitsInAnInt(dictionaryPageOffset)
        && positiveLongFitsInAnInt(valueCount)
        && positiveLongFitsInAnInt(totalSize)
        && positiveLongFitsInAnInt(totalUncompressedSize)) {
      return factory()
          .intColumnChunkMetaData()
          .withPath(path)
          .withPrimitiveType(type)
          .withCodec(codec)
          .withEncodingStats(encodingStats)
          .withEncodings(encodings)
          .withStatistics(statistics)
          .withDictionaryPageOffset(positiveLongToInt(dictionaryPageOffset))
          .withValueCount(positiveLongToInt(valueCount))
          .withTotalSize(positiveLongToInt(totalSize))
          .withTotalUncompressedSize(positiveLongToInt(totalUncompressedSize))
          .withFirstDataPage(positiveLongToInt(firstDataPage))
          .withSizeStatistics(sizeStatistics)
          .build();
    } else {
      return factory()
          .longColumnChunkMetaData()
          .withPath(path)
          .withPrimitiveType(type)
          .withCodec(codec)
          .withEncodingStats(encodingStats)
          .withEncodings(encodings)
          .withStatistics(statistics)
          .withFirstDataPageOffset(firstDataPage)
          .withDictionaryPageOffset(dictionaryPageOffset)
          .withValueCount(valueCount)
          .withTotalSize(totalSize)
          .withTotalUncompressedSize(totalUncompressedSize)
          .withSizeStatistics(sizeStatistics)
          .build();
    }
  }

  // In sensitive columns, the ColumnMetaData structure is encrypted (with column-specific keys), making the fields
  // like Statistics invisible.
  // Decryption is not performed pro-actively, due to performance and authorization reasons.
  // This method creates an a shell ColumnChunkMetaData object that keeps the encrypted metadata and the decryption
  // tools.
  // These tools will activated later - when/if the column is projected.
  public static ColumnChunkMetaData getWithEncryptedMetadata(
      ParquetMetadataConverter parquetMetadataConverter,
      ColumnPath path,
      PrimitiveType type,
      byte[] encryptedMetadata,
      byte[] columnKeyMetadata,
      InternalFileDecryptor fileDecryptor,
      int rowGroupOrdinal,
      int columnOrdinal,
      String createdBy) {

    return factory()
        .encryptedColumnChunkMetaData()
        .withParquetMetadataConverter(parquetMetadataConverter)
        .withPath(path)
        .withPrimitiveType(type)
        .withEncryptedMetadata(encryptedMetadata)
        .withColumnKeyMetadata(columnKeyMetadata)
        .withFileDecryptor(fileDecryptor)
        .withRowGroupOrdinal(rowGroupOrdinal)
        .withColumnOrdinal(columnOrdinal)
        .withCreatedBy(createdBy)
        .build();
  }

  public void setRowGroupOrdinal(int rowGroupOrdinal) {
    this.rowGroupOrdinal = rowGroupOrdinal;
  }

  public int getRowGroupOrdinal() {
    return rowGroupOrdinal;
  }

  /**
   * @return the offset of the first byte in the chunk
   */
  public long getStartingPos() {
    decryptIfNeeded();
    long dictionaryPageOffset = getDictionaryPageOffset();
    long firstDataPageOffset = getFirstDataPageOffset();
    if (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset) {
      // if there's a dictionary and it's before the first data page, start from there
      return dictionaryPageOffset;
    }
    return firstDataPageOffset;
  }

  /**
   * checks that a positive long value fits in an int.
   * (reindexed on Integer.MIN_VALUE)
   *
   * @param value a long value
   * @return whether it fits
   */
  protected static boolean positiveLongFitsInAnInt(long value) {
    return (value >= 0) && (value + Integer.MIN_VALUE <= Integer.MAX_VALUE);
  }

  /**
   * stores a positive long into an int (assuming it fits)
   *
   * @param value
   * @return
   */
  private static int positiveLongToInt(long value) {
    if (!ColumnChunkMetaData.positiveLongFitsInAnInt(value)) {
      throw new IllegalArgumentException("value should be positive and fit in an int: " + value);
    }
    return (int) (value + Integer.MIN_VALUE);
  }

  EncodingStats encodingStats;

  // we save 3 references by storing together the column properties that have few distinct values
  ColumnChunkProperties properties;

  private IndexReference columnIndexReference;
  private IndexReference offsetIndexReference;

  private long bloomFilterOffset = -1;
  private int bloomFilterLength = -1;

  protected ColumnChunkMetaData(Builder<?> builder) {
    this.encodingStats = builder.encodingStats;
    this.properties = builder.properties;
  }

  public abstract static class Builder<T extends Builder<T>> {
    protected EncodingStats encodingStats;
    protected ColumnChunkProperties properties;

    public T withEncodingStats(EncodingStats encodingStats) {
      this.encodingStats = encodingStats;
      return self();
    }

    public T withProperties(ColumnChunkProperties properties) {
      this.properties = properties;
      return self();
    }

    protected abstract T self();

    public abstract ColumnChunkMetaData build();
  }

  public static ColumnChunkMetaDataFactory factory() {
    return new ColumnChunkMetaDataFactory();
  }

  public static class ColumnChunkMetaDataFactory {
    public IntColumnChunkMetaData.Builder intColumnChunkMetaData() {
      return new IntColumnChunkMetaData.Builder();
    }

    public LongColumnChunkMetaData.Builder longColumnChunkMetaData() {
      return new LongColumnChunkMetaData.Builder();
    }

    public EncryptedColumnChunkMetaData.Builder encryptedColumnChunkMetaData() {
      return new EncryptedColumnChunkMetaData.Builder();
    }
  }

  protected ColumnChunkMetaData(ColumnChunkProperties columnChunkProperties) {
    this(null, columnChunkProperties);
  }

  protected ColumnChunkMetaData(EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties) {
    this.encodingStats = encodingStats;
    this.properties = columnChunkProperties;
  }

  protected void decryptIfNeeded() {
    return;
  }

  public CompressionCodecName getCodec() {
    decryptIfNeeded();
    return properties.getCodec();
  }

  /**
   * @return column identifier
   */
  public ColumnPath getPath() {
    return properties.getPath();
  }

  /**
   * @return type of the column
   * @deprecated will be removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   */
  @Deprecated
  @JsonIgnore
  public PrimitiveTypeName getType() {
    decryptIfNeeded();
    return properties.getType();
  }

  /**
   * @return the primitive type object of the column
   */
  public PrimitiveType getPrimitiveType() {
    decryptIfNeeded();
    return properties.getPrimitiveType();
  }

  /**
   * @return start of the column data offset
   */
  public abstract long getFirstDataPageOffset();

  /**
   * @return the location of the dictionary page if any; {@code 0} is returned if there is no dictionary page. Check
   * {@link #hasDictionaryPage()} to validate.
   */
  public abstract long getDictionaryPageOffset();

  /**
   * @return count of values in this block of the column
   */
  public abstract long getValueCount();

  /**
   * @return the totalUncompressedSize
   */
  public abstract long getTotalUncompressedSize();

  /**
   * @return the totalSize
   */
  public abstract long getTotalSize();

  /**
   * @return the stats for this column
   */
  @JsonIgnore
  public abstract Statistics getStatistics();

  /**
   * Method should be considered private
   *
   * @return the size stats for this column
   */
  @JsonIgnore
  public SizeStatistics getSizeStatistics() {
    throw new UnsupportedOperationException("SizeStatistics is not implemented");
  }

  /**
   * Method should be considered private
   *
   * @return the reference to the column index
   */
  public IndexReference getColumnIndexReference() {
    decryptIfNeeded();
    return columnIndexReference;
  }

  /**
   * Method should be considered private
   *
   * @param indexReference the reference to the column index
   */
  public void setColumnIndexReference(IndexReference indexReference) {
    this.columnIndexReference = indexReference;
  }

  /**
   * Method should be considered private
   *
   * @return the reference to the offset index
   */
  public IndexReference getOffsetIndexReference() {
    decryptIfNeeded();
    return offsetIndexReference;
  }

  /**
   * Method should be considered private
   *
   * @param offsetIndexReference the reference to the offset index
   */
  public void setOffsetIndexReference(IndexReference offsetIndexReference) {
    this.offsetIndexReference = offsetIndexReference;
  }

  /**
   * Method should be considered private
   *
   * @param bloomFilterOffset the reference to the Bloom filter
   */
  public void setBloomFilterOffset(long bloomFilterOffset) {
    this.bloomFilterOffset = bloomFilterOffset;
  }

  /**
   * @param bloomFilterLength the reference to the Bloom filter
   */
  public void setBloomFilterLength(int bloomFilterLength) {
    this.bloomFilterLength = bloomFilterLength;
  }

  /**
   * @return the offset to the Bloom filter or {@code -1} if there is no bloom filter for this column chunk
   */
  public long getBloomFilterOffset() {
    decryptIfNeeded();
    return bloomFilterOffset;
  }

  /**
   * @return the length to the Bloom filter or {@code -1} if there is no bloom filter length for this column chunk
   */
  public int getBloomFilterLength() {
    decryptIfNeeded();
    return bloomFilterLength;
  }

  /**
   * @return all the encodings used in this column
   */
  public Set<Encoding> getEncodings() {
    decryptIfNeeded();
    return properties.getEncodings();
  }

  public EncodingStats getEncodingStats() {
    decryptIfNeeded();
    return encodingStats;
  }

  @Override
  public String toString() {
    decryptIfNeeded();
    return "ColumnMetaData{" + properties.toString() + ", " + getFirstDataPageOffset() + "}";
  }

  public boolean hasDictionaryPage() {
    EncodingStats stats = getEncodingStats();
    if (stats != null) {
      // ensure there is a dictionary page and that it is used to encode data pages
      return stats.hasDictionaryPages() && stats.hasDictionaryEncodedPages();
    }

    Set<Encoding> encodings = getEncodings();
    return (encodings.contains(PLAIN_DICTIONARY) || encodings.contains(RLE_DICTIONARY));
  }

  /**
   * Method should be considered private
   *
   * @return whether or not this column is encrypted
   */
  public boolean isEncrypted() {
    return false;
  }
}
