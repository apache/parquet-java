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
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.apache.parquet.schema.MessageType;

/**
 * This class represents all the configurable Parquet properties.
 */
public class ParquetProperties {

  public static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
  public static final int DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final WriterVersion DEFAULT_WRITER_VERSION = WriterVersion.PARQUET_1_0;
  public static final boolean DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK = true;
  public static final int DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  public static final int DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  public static final ValuesWriterFactory DEFAULT_VALUES_WRITER_FACTORY = new DefaultValuesWriterFactory();

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

  private final int initialSlabSize;
  private final int pageSizeThreshold;
  private final int dictionaryPageSizeThreshold;
  private final WriterVersion writerVersion;
  private final boolean enableDictionary;
  private final int minRowCountForPageSizeCheck;
  private final int maxRowCountForPageSizeCheck;
  private final int minRowCountForBlockSizeCheck;
  private final int maxRowCountForBlockSizeCheck;
  private final boolean estimateNextSizeCheck;
  private final ByteBufferAllocator allocator;
  private final ValuesWriterFactory valuesWriterFactory;

  private ParquetProperties(WriterVersion writerVersion, int pageSize, int dictPageSize, boolean enableDict, int minRowCountForPageSizeCheck,
                            int maxRowCountForPageSizeCheck, int minRowCountForBlockSizeCheck, int maxRowCountForBlockSizeCheck,
                            boolean estimateNextSizeCheck, ByteBufferAllocator allocator,
                            ValuesWriterFactory writerFactory) {
    this.pageSizeThreshold = pageSize;
    this.initialSlabSize = CapacityByteArrayOutputStream
      .initialSlabSizeHeuristic(MIN_SLAB_SIZE, pageSizeThreshold, 10);
    this.dictionaryPageSizeThreshold = dictPageSize;
    this.writerVersion = writerVersion;
    this.enableDictionary = enableDict;
    this.minRowCountForPageSizeCheck = minRowCountForPageSizeCheck;
    this.maxRowCountForPageSizeCheck = maxRowCountForPageSizeCheck;
    this.minRowCountForBlockSizeCheck = minRowCountForBlockSizeCheck;
    this.maxRowCountForBlockSizeCheck = maxRowCountForBlockSizeCheck;
    this.estimateNextSizeCheck = estimateNextSizeCheck;
    this.allocator = allocator;

    this.valuesWriterFactory = writerFactory;
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

  public ValuesWriter newValuesWriter(ColumnDescriptor path) {
    return valuesWriterFactory.newValuesWriter(path);
  }

  public int getPageSizeThreshold() {
    return pageSizeThreshold;
  }

  public int getInitialSlabSize() {
    return initialSlabSize;
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

  public int getMinRowCountForBlockSizeCheck() {
    return minRowCountForBlockSizeCheck;
  }

  public int getMaxRowCountForBlockSizeCheck() {
    return maxRowCountForBlockSizeCheck;
  }

  public ValuesWriterFactory getValuesWriterFactory() {
    return valuesWriterFactory;
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
    private int minRowCountForBlockSizeCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
    private int maxRowCountForBlockSizeCheck = DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
    private boolean estimateNextSizeCheck = DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK;
    private ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    private ValuesWriterFactory valuesWriterFactory = DEFAULT_VALUES_WRITER_FACTORY;

    private Builder() {
    }

    private Builder(ParquetProperties toCopy) {
      this.enableDict = toCopy.enableDictionary;
      this.dictPageSize = toCopy.dictionaryPageSizeThreshold;
      this.writerVersion = toCopy.writerVersion;
      this.minRowCountForPageSizeCheck = toCopy.minRowCountForPageSizeCheck;
      this.maxRowCountForPageSizeCheck = toCopy.maxRowCountForPageSizeCheck;
      this.minRowCountForBlockSizeCheck = toCopy.minRowCountForBlockSizeCheck;
      this.maxRowCountForBlockSizeCheck = toCopy.maxRowCountForBlockSizeCheck;
      this.estimateNextSizeCheck = toCopy.estimateNextSizeCheck;
      this.allocator = toCopy.allocator;
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

    public Builder withMinRowCountForBlockSizeCheck(int min) {
      Preconditions.checkArgument(min > 0,
        "Invalid row count for rowgroup size check (negative): %s", min);
      this.minRowCountForBlockSizeCheck = min;
      return this;
    }

    public Builder withMaxRowCountForBlockSizeCheck(int max) {
      Preconditions.checkArgument(max > 0,
        "Invalid row count for rowgroup size check (negative): %s", max);
      this.maxRowCountForBlockSizeCheck = max;
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

    public Builder withValuesWriterFactory(ValuesWriterFactory factory) {
      Preconditions.checkNotNull(factory, "ValuesWriterFactory");
      this.valuesWriterFactory = factory;
      return this;
    }

    public ParquetProperties build() {
      ParquetProperties properties =
        new ParquetProperties(writerVersion, pageSize, dictPageSize,
          enableDict, minRowCountForPageSizeCheck, maxRowCountForPageSizeCheck,
          minRowCountForBlockSizeCheck, maxRowCountForBlockSizeCheck,
          estimateNextSizeCheck, allocator, valuesWriterFactory);
      // we pass a constructed but uninitialized factory to ParquetProperties above as currently
      // creation of ValuesWriters is invoked from within ParquetProperties. In the future
      // we'd like to decouple that and won't need to pass an object to properties and then pass the
      // properties to the object.
      valuesWriterFactory.initialize(properties);

      return properties;
    }

  }
}
