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
package org.apache.parquet.column.values;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

public class ValuesWriterFactory {

  private final WriterVersion writerVersion;

  private final boolean enableDictionary;
  private final int initialSlabSize;
  private final int pageSizeThreshold;
  private final ByteBufferAllocator allocator;
  private final int dictionaryPageSizeThreshold;

  public ValuesWriterFactory(WriterVersion writerVersion, int initialSlabSize, int pageSizeThreshold,
                             ByteBufferAllocator allocator, int dictionaryPageSizeThreshold,
                             boolean enableDictionary) {
    this.writerVersion = writerVersion;
    this.initialSlabSize = initialSlabSize;
    this.pageSizeThreshold = pageSizeThreshold;
    this.allocator = allocator;
    this.dictionaryPageSizeThreshold = dictionaryPageSizeThreshold;
    this.enableDictionary = enableDictionary;
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
        return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case INT32:
        return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case INT64:
        return new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case INT96:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, 12, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case DOUBLE:
        return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case FLOAT:
        return new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(dictionaryPageSizeThreshold, encodingForDataPage, encodingForDictionaryPage, this.allocator);
      case FIXED_LEN_BYTE_ARRAY:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(dictionaryPageSizeThreshold, path.getTypeLength(), encodingForDataPage, encodingForDictionaryPage, this.allocator);
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
}
