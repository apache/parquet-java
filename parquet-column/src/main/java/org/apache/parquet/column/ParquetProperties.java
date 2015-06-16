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
package org.apache.parquet.column;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.statistics.StatisticsOpts;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.boundedint.DevNullValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.MessageType;

/**
 * This class represents all the configurable Parquet properties.
 *
 * @author amokashi
 *
 */
public class ParquetProperties {

  public static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
  public static final int DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final WriterVersion DEFAULT_WRITER_VERSION = WriterVersion.PARQUET_1_0;
  public static final boolean DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK = true;
  public static final int DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  public static final int DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private static final int MIN_SLAB_SIZE = 64;

  public enum WriterVersion {
    PARQUET_1_0 ("v1"),
    PARQUET_2_0 ("v2");

    private final String shortName;

    WriterVersion(String shortname) {
      this.shortName = shortname;
    }

    public static WriterVersion fromString(String name) {
      for (WriterVersion v : WriterVersion.values()) {
        if (v.shortName.equals(name)) {
          return v;
        }
      }
      // Throws IllegalArgumentException if name does not exact match with enum name
      return WriterVersion.valueOf(name);
    }
  }

  private final int pageSizeThreshold;
  private final int dictionaryPageSizeThreshold;
  private final WriterVersion writerVersion;
  private final boolean enableDictionary;
  private final int minRowCountForPageSizeCheck;
  private final int maxRowCountForPageSizeCheck;
  private final boolean estimateNextSizeCheck;
  private final ByteBufferAllocator allocator;
  private final StatisticsOpts statisticsOpts;

  private final int initialSlabSize;

  private ParquetProperties(WriterVersion writerVersion, int pageSize, int dictPageSize, boolean enableDict, int minRowCountForPageSizeCheck,
                            int maxRowCountForPageSizeCheck, boolean estimateNextSizeCheck, ByteBufferAllocator allocator, StatisticsOpts statisticsOpts) {
    this.pageSizeThreshold = pageSize;
    this.initialSlabSize = CapacityByteArrayOutputStream
        .initialSlabSizeHeuristic(MIN_SLAB_SIZE, pageSizeThreshold, 10);
    this.dictionaryPageSizeThreshold = dictPageSize;
    this.writerVersion = writerVersion;
    this.enableDictionary = enableDict;
    this.minRowCountForPageSizeCheck = minRowCountForPageSizeCheck;
    this.maxRowCountForPageSizeCheck = maxRowCountForPageSizeCheck;
    this.estimateNextSizeCheck = estimateNextSizeCheck;
    this.statisticsOpts = statisticsOpts;
    this.allocator = allocator;
  }

  public ValuesWriter newRepetitionLevelWriter(ColumnDescriptor path) {
    return newColumnDescriptorValuesWriter(path.getMaxRepetitionLevel());
  }

  public ValuesWriter newDefinitionLevelWriter(ColumnDescriptor path) {
    return newColumnDescriptorValuesWriter(path.getMaxDefinitionLevel());
  }

  private ValuesWriter newColumnDescriptorValuesWriter(int maxLevel) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator);
    }
  }

  public RunLengthBitPackingHybridEncoder newRepetitionLevelEncoder(ColumnDescriptor path) {
    return newLevelEncoder(path.getMaxRepetitionLevel());
  }

  public RunLengthBitPackingHybridEncoder newDefinitionLevelEncoder(ColumnDescriptor path) {
    return newLevelEncoder(path.getMaxDefinitionLevel());
  }

  private RunLengthBitPackingHybridEncoder newLevelEncoder(int maxLevel) {
    return new RunLengthBitPackingHybridEncoder(
        getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator);
  }

  private ValuesWriter plainWriter(ColumnDescriptor path) {
    switch (path.getType()) {
    case BOOLEAN:
      return new BooleanPlainValuesWriter();
    case INT96:
      return new FixedLenByteArrayPlainValuesWriter(12, initialSlabSize, pageSizeThreshold, allocator);
    case FIXED_LEN_BYTE_ARRAY:
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSlabSize, pageSizeThreshold, allocator);
    case BINARY:
    case INT32:
    case INT64:
    case DOUBLE:
    case FLOAT:
      return new PlainValuesWriter(initialSlabSize, pageSizeThreshold, allocator);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  @SuppressWarnings("deprecation")
  private DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path) {
    Encoding encodingForDataPage;
    Encoding encodingForDictionaryPage;
    switch(writerVersion) {
    case PARQUET_1_0:
      encodingForDataPage = PLAIN_DICTIONARY;
      encodingForDictionaryPage = PLAIN_DICTIONARY;
      break;
    case PARQUET_2_0:
      encodingForDataPage = RLE_DICTIONARY;
      encodingForDictionaryPage = PLAIN;
      break;
    default:
      throw new IllegalArgumentException("Unknown version: " + writerVersion);
    }
    switch (path.getType()) {
    case BOOLEAN:
      throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
    case BINARY:
      return new PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case INT32:
      return new PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case INT64:
      return new PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case INT96:
      return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, 12, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case DOUBLE:
      return new PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case FLOAT:
      return new PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
    case FIXED_LEN_BYTE_ARRAY:
      return new PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, path.getTypeLength(), encodingForDataPage, encodingForDictionaryPage, this.allocator);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  private ValuesWriter writerToFallbackTo(ColumnDescriptor path) {
    switch(writerVersion) {
    case PARQUET_1_0:
      return plainWriter(path);
    case PARQUET_2_0:
      switch (path.getType()) {
      case BOOLEAN:
        return new RunLengthBitPackingHybridValuesWriter(1, initialSlabSize, pageSizeThreshold, allocator);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return new DeltaByteArrayWriter(initialSlabSize, pageSizeThreshold, allocator);
      case INT32:
        return new DeltaBinaryPackingValuesWriterForInteger(initialSlabSize, pageSizeThreshold, allocator);
      case INT64:
        return new DeltaBinaryPackingValuesWriterForLong(initialSlabSize, pageSizeThreshold, allocator);
      case INT96:
      case DOUBLE:
      case FLOAT:
        return plainWriter(path);
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
      }
    default:
      throw new IllegalArgumentException("Unknown version: " + writerVersion);
    }
  }

  private ValuesWriter dictWriterWithFallBack(ColumnDescriptor path) {
    ValuesWriter writerToFallBackTo = writerToFallbackTo(path);
    if (enableDictionary) {
      return FallbackValuesWriter.of(
          dictionaryWriter(path),
          writerToFallBackTo);
    } else {
     return writerToFallBackTo;
    }
  }

  public ValuesWriter newValuesWriter(ColumnDescriptor path) {
    switch (path.getType()) {
    case BOOLEAN: // no dictionary encoding for boolean
      return writerToFallbackTo(path);
    case FIXED_LEN_BYTE_ARRAY:
      // dictionary encoding for that type was not enabled in PARQUET 1.0
      if (writerVersion == WriterVersion.PARQUET_2_0) {
        return dictWriterWithFallBack(path);
      } else {
       return writerToFallbackTo(path);
      }
    case BINARY:
    case INT32:
    case INT64:
    case INT96:
    case DOUBLE:
    case FLOAT:
      return dictWriterWithFallBack(path);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  public int getPageSizeThreshold() {
    return pageSizeThreshold;
  }

  public int getDictionaryPageSizeThreshold() {
    return dictionaryPageSizeThreshold;
  }

  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  public boolean isEnableDictionary() {
    return enableDictionary;
  }

  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  public StatisticsOpts getStatisticsOpts(){
    return statisticsOpts;
  }

  public ColumnWriteStore newColumnWriteStore(MessageType schema,
                                              PageWriteStore pageStore) {
    switch (writerVersion) {
    case PARQUET_1_0:
      return new ColumnWriteStoreV1(pageStore, this);
    case PARQUET_2_0:
      return new ColumnWriteStoreV2(schema, pageStore, this);
    default:
      throw new IllegalArgumentException("unknown version " + writerVersion);
    }
  }

  public int getMinRowCountForPageSizeCheck() {
    return minRowCountForPageSizeCheck;
  }

  public int getMaxRowCountForPageSizeCheck() {
    return maxRowCountForPageSizeCheck;
  }

  public boolean estimateNextSizeCheck() {
    return estimateNextSizeCheck;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder copy(ParquetProperties toCopy) {
    return new Builder(toCopy);
  }

  public static class Builder {
    private int pageSize = DEFAULT_PAGE_SIZE;
    private int dictPageSize = DEFAULT_DICTIONARY_PAGE_SIZE;
    private boolean enableDict = DEFAULT_IS_DICTIONARY_ENABLED;
    private WriterVersion writerVersion = DEFAULT_WRITER_VERSION;
    private int minRowCountForPageSizeCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
    private int maxRowCountForPageSizeCheck = DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
    private boolean estimateNextSizeCheck = DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK;
    private ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    private StatisticsOpts statisticsOpts = new StatisticsOpts(null);

    private Builder() {
    }

    private Builder(ParquetProperties toCopy) {
      this.enableDict = toCopy.enableDictionary;
      this.dictPageSize = toCopy.dictionaryPageSizeThreshold;
      this.writerVersion = toCopy.writerVersion;
      this.minRowCountForPageSizeCheck = toCopy.minRowCountForPageSizeCheck;
      this.maxRowCountForPageSizeCheck = toCopy.maxRowCountForPageSizeCheck;
      this.estimateNextSizeCheck = toCopy.estimateNextSizeCheck;
      this.allocator = toCopy.allocator;
      this.statisticsOpts = toCopy.statisticsOpts;
    }

    /**
     * Set the Parquet format page size.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public Builder withPageSize(int pageSize) {
      Preconditions.checkArgument(pageSize > 0,
          "Invalid page size (negative): %s", pageSize);
      this.pageSize = pageSize;
      return this;
    }

    /**
     * Enable or disable dictionary encoding.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public Builder withDictionaryEncoding(boolean enableDictionary) {
      this.enableDict = enableDictionary;
      return this;
    }

    /**
     * Set the Parquet format dictionary page size.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public Builder withDictionaryPageSize(int dictionaryPageSize) {
      Preconditions.checkArgument(dictionaryPageSize > 0,
          "Invalid dictionary page size (negative): %s", dictionaryPageSize);
      this.dictPageSize = dictionaryPageSize;
      return this;
    }

    /**
     * Set the {@link WriterVersion format version}.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public Builder withWriterVersion(WriterVersion version) {
      this.writerVersion = version;
      return this;
    }

    public Builder withMinRowCountForPageSizeCheck(int min) {
      Preconditions.checkArgument(min > 0,
          "Invalid row count for page size check (negative): %s", min);
      this.minRowCountForPageSizeCheck = min;
      return this;
    }

    public Builder withMaxRowCountForPageSizeCheck(int max) {
      Preconditions.checkArgument(max > 0,
          "Invalid row count for page size check (negative): %s", max);
      this.maxRowCountForPageSizeCheck = max;
      return this;
    }

    // Do not attempt to predict next size check.  Prevents issues with rows that vary significantly in size.
    public Builder estimateRowCountForPageSizeCheck(boolean estimateNextSizeCheck) {
      this.estimateNextSizeCheck = estimateNextSizeCheck;
      return this;
    }

    public Builder withAllocator(ByteBufferAllocator allocator) {
      Preconditions.checkNotNull(allocator, "ByteBufferAllocator");
      this.allocator = allocator;
      return this;
    }

    public Builder withStatisticsOpts(StatisticsOpts statisticsOpts){
      this.statisticsOpts = statisticsOpts;
      return this;
    }

    public ParquetProperties build() {
      return new ParquetProperties(writerVersion, pageSize, dictPageSize,
          enableDict, minRowCountForPageSizeCheck, maxRowCountForPageSizeCheck,
          estimateNextSizeCheck, allocator, statisticsOpts);
    }
  }
}
