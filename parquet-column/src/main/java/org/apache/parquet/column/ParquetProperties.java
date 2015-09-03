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
import org.apache.parquet.bytes.HeapByteBufferAllocator;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.boundedint.DevNullValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
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
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.MessageType;

/**
 * This class represents all the configurable Parquet properties.
 *
 * @author amokashi
 *
 */
public class ParquetProperties {

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
  private final int dictionaryPageSizeThreshold;
  private final WriterVersion writerVersion;
  private final boolean enableDictionary;
  // TODO - Should this even hold an allocator? this isn't really a property
  private ByteBufferAllocator allocator;

  public ParquetProperties(int dictPageSize, WriterVersion writerVersion, boolean enableDict) {
    this(dictPageSize, writerVersion, enableDict, new HeapByteBufferAllocator());
  }

  public ParquetProperties(int dictPageSize, WriterVersion writerVersion, boolean enableDict, ByteBufferAllocator allocator) {
    this.dictionaryPageSizeThreshold = dictPageSize;
    this.writerVersion = writerVersion;
    this.enableDictionary = enableDict;
    Preconditions.checkNotNull(allocator, "ByteBufferAllocator");
    this.allocator = allocator;
  }

  public ValuesWriter getColumnDescriptorValuesWriter(int maxLevel, int initialSizePerCol, int pageSize) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          getWidthFromMaxInt(maxLevel), initialSizePerCol, pageSize, this.allocator
      );
    }
  }

  private ValuesWriter plainWriter(ColumnDescriptor path, int initialSizePerCol, int pageSize) {
    switch (path.getType()) {
    case BOOLEAN:
      return new BooleanPlainValuesWriter();
    case INT96:
      return new FixedLenByteArrayPlainValuesWriter(12, initialSizePerCol, pageSize, this.allocator);
    case FIXED_LEN_BYTE_ARRAY:
      return new FixedLenByteArrayPlainValuesWriter(path.getTypeLength(), initialSizePerCol, pageSize, this.allocator);
    case BINARY:
    case INT32:
    case INT64:
    case DOUBLE:
    case FLOAT:
      return new PlainValuesWriter(initialSizePerCol, pageSize, this.allocator);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  private DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, int initialSizePerCol) {
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

  private ValuesWriter writerToFallbackTo(ColumnDescriptor path, int initialSizePerCol, int pageSize) {
    switch(writerVersion) {
    case PARQUET_1_0:
      return plainWriter(path, initialSizePerCol, pageSize);
    case PARQUET_2_0:
      switch (path.getType()) {
      case BOOLEAN:
        return new RunLengthBitPackingHybridValuesWriter(1, initialSizePerCol, pageSize, this.allocator);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return new DeltaByteArrayWriter(initialSizePerCol, pageSize,this.allocator);
      case INT32:
        return new DeltaBinaryPackingValuesWriter(initialSizePerCol, pageSize, this.allocator);
      case INT96:
      case INT64:
      case DOUBLE:
      case FLOAT:
        return plainWriter(path, initialSizePerCol, pageSize);
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
      }
    default:
      throw new IllegalArgumentException("Unknown version: " + writerVersion);
    }
  }

  private ValuesWriter dictWriterWithFallBack(ColumnDescriptor path, int initialSizePerCol, int pageSize) {
    ValuesWriter writerToFallBackTo = writerToFallbackTo(path, initialSizePerCol, pageSize);
    if (enableDictionary) {
      return FallbackValuesWriter.of(
          dictionaryWriter(path, initialSizePerCol),
          writerToFallBackTo);
    } else {
     return writerToFallBackTo;
    }
  }

  public ValuesWriter getValuesWriter(ColumnDescriptor path, int initialSizePerCol, int pageSize) {
    switch (path.getType()) {
    case BOOLEAN: // no dictionary encoding for boolean
      return writerToFallbackTo(path, initialSizePerCol, pageSize);
    case FIXED_LEN_BYTE_ARRAY:
      // dictionary encoding for that type was not enabled in PARQUET 1.0
      if (writerVersion == WriterVersion.PARQUET_2_0) {
        return dictWriterWithFallBack(path, initialSizePerCol, pageSize);
      } else {
       return writerToFallbackTo(path, initialSizePerCol, pageSize);
      }
    case BINARY:
    case INT32:
    case INT64:
    case INT96:
    case DOUBLE:
    case FLOAT:
      return dictWriterWithFallBack(path, initialSizePerCol, pageSize);
    default:
      throw new IllegalArgumentException("Unknown type " + path.getType());
    }
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

  public ColumnWriteStore newColumnWriteStore(
      MessageType schema,
      PageWriteStore pageStore,
      int pageSize,
      ByteBufferAllocator allocator) {
    switch (writerVersion) {
    case PARQUET_1_0:
      return new ColumnWriteStoreV1(
          pageStore,
          pageSize,
          dictionaryPageSizeThreshold,
          enableDictionary, writerVersion, allocator);
    case PARQUET_2_0:
      return new ColumnWriteStoreV2(
          schema,
          pageStore,
          pageSize,
          new ParquetProperties(dictionaryPageSizeThreshold, writerVersion, enableDictionary),
          allocator);
    default:
      throw new IllegalArgumentException("unknown version " + writerVersion);
    }
  }
}
