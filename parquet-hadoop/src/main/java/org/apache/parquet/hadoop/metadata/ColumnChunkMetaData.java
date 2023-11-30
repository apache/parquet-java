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
import static org.apache.parquet.format.Util.readColumnMetaData;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.format.ColumnMetaData;
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
          totalUncompressedSize);
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
          totalUncompressedSize);
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

  EncodingStats encodingStats;

  // we save 3 references by storing together the column properties that have few distinct values
  ColumnChunkProperties properties;

  private IndexReference columnIndexReference;
  private IndexReference offsetIndexReference;

  private long bloomFilterOffset = -1;

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
  public abstract Statistics getStatistics();

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
   * Method should be considered private
   *
   * @return the offset to the Bloom filter or {@code -1} if there is no bloom filter for this column chunk
   */
  public long getBloomFilterOffset() {
    decryptIfNeeded();
    return bloomFilterOffset;
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

class IntColumnChunkMetaData extends ColumnChunkMetaData {

  private final int firstDataPage;
  private final int dictionaryPageOffset;
  private final int valueCount;
  private final int totalSize;
  private final int totalUncompressedSize;
  private final Statistics statistics;

  /**
   * @param path                  column identifier
   * @param type                  type of the column
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
   * @param path                  column identifier
   * @param type                  type of the column
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

class EncryptedColumnChunkMetaData extends ColumnChunkMetaData {
  private final ParquetMetadataConverter parquetMetadataConverter;
  private final byte[] encryptedMetadata;
  private final byte[] columnKeyMetadata;
  private final InternalFileDecryptor fileDecryptor;

  private final int columnOrdinal;
  private final PrimitiveType primitiveType;
  private final String createdBy;
  private ColumnPath path;

  private boolean decrypted;
  private ColumnChunkMetaData shadowColumnChunkMetaData;

  EncryptedColumnChunkMetaData(
      ParquetMetadataConverter parquetMetadataConverter,
      ColumnPath path,
      PrimitiveType type,
      byte[] encryptedMetadata,
      byte[] columnKeyMetadata,
      InternalFileDecryptor fileDecryptor,
      int rowGroupOrdinal,
      int columnOrdinal,
      String createdBy) {
    super((EncodingStats) null, (ColumnChunkProperties) null);
    this.parquetMetadataConverter = parquetMetadataConverter;
    this.path = path;
    this.encryptedMetadata = encryptedMetadata;
    this.columnKeyMetadata = columnKeyMetadata;
    this.fileDecryptor = fileDecryptor;
    this.rowGroupOrdinal = rowGroupOrdinal;
    this.columnOrdinal = columnOrdinal;
    this.primitiveType = type;
    this.createdBy = createdBy;

    this.decrypted = false;
  }

  @Override
  protected void decryptIfNeeded() {
    if (decrypted) return;

    if (null == fileDecryptor) {
      throw new ParquetCryptoRuntimeException(path + ". Null File Decryptor");
    }

    // Decrypt the ColumnMetaData
    InternalColumnDecryptionSetup columnDecryptionSetup =
        fileDecryptor.setColumnCryptoMetadata(path, true, false, columnKeyMetadata, columnOrdinal);

    ColumnMetaData metaData;
    ByteArrayInputStream tempInputStream = new ByteArrayInputStream(encryptedMetadata);
    byte[] columnMetaDataAAD = AesCipher.createModuleAAD(
        fileDecryptor.getFileAAD(), ModuleType.ColumnMetaData, rowGroupOrdinal, columnOrdinal, -1);
    try {
      metaData = readColumnMetaData(
          tempInputStream, columnDecryptionSetup.getMetaDataDecryptor(), columnMetaDataAAD);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException(path + ". Failed to decrypt column metadata", e);
    }
    decrypted = true;
    shadowColumnChunkMetaData =
        parquetMetadataConverter.buildColumnChunkMetaData(metaData, path, primitiveType, createdBy);
    this.encodingStats = shadowColumnChunkMetaData.encodingStats;
    this.properties = shadowColumnChunkMetaData.properties;
    if (metaData.isSetBloom_filter_offset()) {
      setBloomFilterOffset(metaData.getBloom_filter_offset());
    }
  }

  @Override
  public ColumnPath getPath() {
    return path;
  }

  @Override
  public long getFirstDataPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getFirstDataPageOffset();
  }

  @Override
  public long getDictionaryPageOffset() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getDictionaryPageOffset();
  }

  @Override
  public long getValueCount() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getValueCount();
  }

  @Override
  public long getTotalUncompressedSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalUncompressedSize();
  }

  @Override
  public long getTotalSize() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getTotalSize();
  }

  @Override
  public Statistics getStatistics() {
    decryptIfNeeded();
    return shadowColumnChunkMetaData.getStatistics();
  }

  /**
   * @return whether or not this column is encrypted
   */
  @Override
  public boolean isEncrypted() {
    return true;
  }
}
