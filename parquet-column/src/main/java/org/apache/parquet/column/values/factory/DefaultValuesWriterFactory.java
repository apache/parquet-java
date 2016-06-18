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
package org.apache.parquet.column.values.factory;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;

/**
 * Handles ValuesWriter creation statically based on the types of the columns and the writer version.
 */
public class DefaultValuesWriterFactory implements ValuesWriterFactory {

  private ValuesWriterFactoryParams selectionParams;
  private ValuesWriterFactory delegateFactory;

  private static final ValuesWriterFactory DEFAULT_V1_WRITER_FACTORY = new DefaultV1ValuesWriterFactory();
  private static final ValuesWriterFactory DEFAULT_V2_WRITER_FACTORY = new DefaultV2ValuesWriterFactory();

  @Override
  public void initialize(ValuesWriterFactoryParams params) {
    this.selectionParams = params;
    if (selectionParams.getWriterVersion() == WriterVersion.PARQUET_1_0) {
      delegateFactory = DEFAULT_V1_WRITER_FACTORY;
    } else {
      delegateFactory = DEFAULT_V2_WRITER_FACTORY;
    }

    delegateFactory.initialize(params);
  }

  @Override
  public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
    return delegateFactory.newValuesWriter(descriptor);
  }

  public static DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, ValuesWriterFactoryParams selectionParams, Encoding dictPageEncoding, Encoding dataPageEncoding) {
    switch (path.getType()) {
      case BOOLEAN:
        throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
      case BINARY:
        return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case INT32:
        return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case INT64:
        return new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case INT96:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), 12, dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case DOUBLE:
        return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case FLOAT:
        return new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      case FIXED_LEN_BYTE_ARRAY:
        return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(selectionParams.getMaxDictionaryByteSize(), path.getTypeLength(), dataPageEncoding, dictPageEncoding, selectionParams.getAllocator());
      default:
        throw new IllegalArgumentException("Unknown type " + path.getType());
    }
  }

  public static ValuesWriter dictWriterWithFallBack(ColumnDescriptor path, ValuesWriterFactoryParams selectionParams, Encoding dictPageEncoding, Encoding dataPageEncoding, ValuesWriter writerToFallBackTo) {
    if (selectionParams.getEnableDictionary()) {
      return FallbackValuesWriter.of(
        dictionaryWriter(path, selectionParams, dictPageEncoding, dataPageEncoding),
        writerToFallBackTo);
    } else {
      return writerToFallBackTo;
    }
  }
}
