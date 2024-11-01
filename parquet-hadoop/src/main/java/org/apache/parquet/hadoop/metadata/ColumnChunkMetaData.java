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
import java.util.function.Supplier;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
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
  protected EncodingStats encodingStats;
  protected ColumnChunkProperties properties;

  private IndexReference columnIndexReference;
  private IndexReference offsetIndexReference;
  private long bloomFilterOffset = -1;
  private int bloomFilterLength = -1;

  protected ColumnChunkMetaData(ColumnChunkProperties columnChunkProperties) {
    this(null, columnChunkProperties);
  }

  protected ColumnChunkMetaData(EncodingStats encodingStats, ColumnChunkProperties columnChunkProperties) {
    this.encodingStats = encodingStats;
    this.properties = columnChunkProperties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ColumnPath path;
    private PrimitiveType type;
    private CompressionCodecName codec;
    private EncodingStats encodingStats;
    private Set<Encoding> encodings;
    private Statistics statistics;
    private long firstDataPage;
    private long dictionaryPageOffset;
    private long valueCount;
    private long totalSize;
    private long totalUncompressedSize;
    private SizeStatistics sizeStatistics;

    public Builder setPath(ColumnPath path) {
      this.path = path;
      return this;
    }

    public Builder setType(PrimitiveType type) {
      this.type = type;
      return this;
    }

    public Builder setCodec(CompressionCodecName codec) {
      this.codec = codec;
      return this;
    }

    public Builder setEncodingStats(EncodingStats encodingStats) {
      this.encodingStats = encodingStats;
      return this;
    }

    public Builder setEncodings(Set<Encoding> encodings) {
      this.encodings = encodings;
      return this;
    }

    public Builder setStatistics(Statistics statistics) {
      this.statistics = statistics;
      return this;
    }

    public Builder setFirstDataPage(long firstDataPage) {
      this.firstDataPage = firstDataPage;
      return this;
    }

    public Builder setDictionaryPageOffset(long dictionaryPageOffset) {
      this.dictionaryPageOffset = dictionaryPageOffset;
      return this;
    }

    public Builder setValueCount(long valueCount) {
      this.valueCount = valueCount;
      return this;
    }

    public Builder setTotalSize(long totalSize) {
      this.totalSize = totalSize;
      return this;
    }

    public Builder setTotalUncompressedSize(long totalUncompressedSize) {
      this.totalUncompressedSize = totalUncompressedSize;
      return this;
    }

    public Builder setSizeStatistics(SizeStatistics sizeStatistics) {
      this.sizeStatistics = sizeStatistics;
      return this;
    }

    public ColumnChunkMetaData build() {
      // to save space we store those always positive longs in ints when they fit.
      if (positiveLongFitsInAnInt(firstDataPage)
          && positiveLongFitsInAnInt(dictionaryPageOffset)
          && positiveLongFitsInAnInt(valueCount)
          && positiveLongFitsInAnInt(totalSize)
          && positiveLongFitsInAnInt(totalUncompressedSize)) {
        return new IntColumnChunkMetaData(
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
            sizeStatistics);
      } else {
        return new LongColumnChunkMetaData(
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
            sizeStatistics);
      }
    }
  }

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
    return builder()
        .setPath(path)
        .setType(Types.optional(type).named("fake_type"))
        .setCodec(codec)
        .setEncodings(encodings)
        .setFirstDataPage(firstDataPage)
        .setDictionaryPageOffset(dictionaryPageOffset)
        .setValueCount(valueCount)
        .setTotalSize(totalSize)
        .setTotalUncompressedSize(totalUncompressedSize)
        .build();
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
    return builder()
        .setPath(path)
        .setType(Types.optional(type).named("fake_type"))
        .setCodec(codec)
        .setEncodingStats(encodingStats)
        .setEncodings(encodings)
        .setStatistics(statistics)
        .setFirstDataPage(firstDataPage)
        .setDictionaryPageOffset(dictionaryPageOffset)
        .setValueCount(valueCount)
        .setTotalSize(totalSize)
        .setTotalUncompressedSize(totalUncompressedSize)
        .build();
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
    return builder()
        .setPath(path)
        .setType(Types.optional(type).named("fake_type"))
        .setCodec(codec)
        .setEncodings(encodings)
        .setStatistics(statistics)
        .setFirstDataPage(firstDataPage)
        .setDictionaryPageOffset(dictionaryPageOffset)
        .setValueCount(valueCount)
        .setTotalSize(totalSize)
        .setTotalUncompressedSize(totalUncompressedSize)
        .build();
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
    return builder()
        .setPath(path)
        .setType(type)
        .setCodec(codec)
        .setEncodingStats(encodingStats)
        .setEncodings(encodings)
        .setStatistics(statistics)
        .setFirstDataPage(firstDataPage)
        .setDictionaryPageOffset(dictionaryPageOffset)
        .setValueCount(valueCount)
        .setTotalSize(totalSize)
        .setTotalUncompressedSize(totalUncompressedSize)
        .build();
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
      long totalUncompressedSize,
      SizeStatistics sizeStatistics) {
    return builder()
        .setPath(path)
        .setType(type)
        .setCodec(codec)
        .setEncodingStats(encodingStats)
        .setEncodings(encodings)
        .setStatistics(statistics)
        .setFirstDataPage(firstDataPage)
        .setDictionaryPageOffset(dictionaryPageOffset)
        .setValueCount(valueCount)
        .setTotalSize(totalSize)
        .setTotalUncompressedSize(totalUncompressedSize)
        .setSizeStatistics(sizeStatistics)
        .build();
  }

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
    return new EncryptedColumnChunkMetaData(
        parquetMetadataConverter,
        path,
        type,
        encryptedMetadata,
        columnKeyMetadata,
        fileDecryptor,
        rowGroupOrdinal,
        columnOrdinal,
        createdBy);
  }

  public void setRowGroupOrdinal(int rowGroupOrdinal) {
    this.rowGroupOrdinal = rowGroupOrdinal;
  }

  public int getRowGroupOrdinal() {
    return rowGroupOrdinal;
  }

  public long getStartingPos() {
    return decryptAndReturn(() -> {
      long dictionaryPageOffset = getDictionaryPageOffsetWithDecrypt();
      long firstDataPageOffset = getFirstDataPageOffset();
      return (dictionaryPageOffset > 0 && dictionaryPageOffset < firstDataPageOffset)
          ? dictionaryPageOffset
          : firstDataPageOffset;
    });
  }

  public CompressionCodecName getCodec() {
    return decryptAndReturn(() -> properties.getCodec());
  }

  public ColumnPath getPath() {
    return properties.getPath();
  }

  @JsonIgnore
  @Deprecated
  public PrimitiveTypeName getType() {
    return decryptAndReturn(() -> properties.getType());
  }

  public PrimitiveType getPrimitiveType() {
    return decryptAndReturn(() -> properties.getPrimitiveType());
  }

  public abstract long getFirstDataPageOffset();

  public abstract long getDictionaryPageOffset();

  public abstract long getValueCount();

  public abstract long getTotalUncompressedSize();

  public abstract long getTotalSize();

  @JsonIgnore
  public abstract Statistics getStatistics();

  @JsonIgnore
  public SizeStatistics getSizeStatistics() {
    throw new UnsupportedOperationException("SizeStatistics is not implemented");
  }

  public long getFirstDataPageOffsetWithDecrypt() {
    return decryptAndReturn(this::getFirstDataPageOffset);
  }

  public long getDictionaryPageOffsetWithDecrypt() {
    return decryptAndReturn(this::getDictionaryPageOffset);
  }

  public long getValueCountWithDecrypt() {
    return decryptAndReturn(this::getValueCount);
  }

  public long getTotalUncompressedSizeWithDecrypt() {
    return decryptAndReturn(this::getTotalUncompressedSize);
  }

  public long getTotalSizeWithDecrypt() {
    return decryptAndReturn(this::getTotalSize);
  }

  public Statistics getStatisticsWithDecrypt() {
    return decryptAndReturn(this::getStatistics);
  }

  public SizeStatistics getSizeStatisticsWithDecrypt() {
    return decryptAndReturn(this::getSizeStatistics);
  }

  public IndexReference getColumnIndexReference() {
    return decryptAndReturn(() -> columnIndexReference);
  }

  public void setColumnIndexReference(IndexReference indexReference) {
    this.columnIndexReference = indexReference;
  }

  public IndexReference getOffsetIndexReference() {
    return decryptAndReturn(() -> offsetIndexReference);
  }

  public void setOffsetIndexReference(IndexReference offsetIndexReference) {
    this.offsetIndexReference = offsetIndexReference;
  }

  public void setBloomFilterOffset(long bloomFilterOffset) {
    this.bloomFilterOffset = bloomFilterOffset;
  }

  public void setBloomFilterLength(int bloomFilterLength) {
    this.bloomFilterLength = bloomFilterLength;
  }

  public long getBloomFilterOffset() {
    return decryptAndReturn(() -> bloomFilterOffset);
  }

  public int getBloomFilterLength() {
    return decryptAndReturn(() -> bloomFilterLength);
  }

  public Set<Encoding> getEncodings() {
    return decryptAndReturn(properties::getEncodings);
  }

  public EncodingStats getEncodingStats() {
    return decryptAndReturn(() -> encodingStats);
  }

  @Override
  public String toString() {
    return decryptAndReturn(
        () -> String.format("ColumnMetaData{%s, %d}", properties, getFirstDataPageOffsetWithDecrypt()));
  }

  private <T> T decryptAndReturn(Supplier<T> supplier) {
    decryptIfNeeded();
    return supplier.get();
  }

  /**
   * Checks if the column has a dictionary page.
   *
   * @return true if the column has a dictionary page, false otherwise
   */
  public boolean hasDictionaryPage() {
    EncodingStats stats = getEncodingStats();
    if (stats != null) {
      return stats.hasDictionaryPages() && stats.hasDictionaryEncodedPages();
    }
    Set<Encoding> encodings = getEncodings();
    return (encodings.contains(PLAIN_DICTIONARY) || encodings.contains(RLE_DICTIONARY));
  }

  public boolean isEncrypted() {
    return false;
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

  protected void decryptIfNeeded() {
    // Decrypt if necessary
  }
}
