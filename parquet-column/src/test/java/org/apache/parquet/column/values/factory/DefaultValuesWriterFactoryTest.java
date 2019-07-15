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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.*;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultValuesWriterFactoryTest {

  @Test
  public void testBoolean() {
    doTestValueWriter(
      PrimitiveTypeName.BOOLEAN,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      BooleanPlainValuesWriter.class);
  }

  @Test
  public void testBoolean_V2() {
    doTestValueWriter(
      PrimitiveTypeName.BOOLEAN,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      RunLengthBitPackingHybridValuesWriter.class);
  }

  @Test
  public void testFixedLenByteArray() {
    doTestValueWriter(
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testFixedLenByteArray_V2() {
    doTestValueWriter(
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      DictionaryValuesWriter.class, DeltaByteArrayWriter.class);
  }

  @Test
  public void testFixedLenByteArray_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      DeltaByteArrayWriter.class);
  }

  @Test
  public void testBinary() {
    doTestValueWriter(
      PrimitiveTypeName.BINARY,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainBinaryDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testBinary_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.BINARY,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testBinary_V2() {
    doTestValueWriter(
      PrimitiveTypeName.BINARY,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainBinaryDictionaryValuesWriter.class, DeltaByteArrayWriter.class);
  }

  @Test
  public void testBinary_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.BINARY,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      DeltaByteArrayWriter.class);
  }

  @Test
  public void testInt32() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainIntegerDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testInt32_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testInt32_V2() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainIntegerDictionaryValuesWriter.class, DeltaBinaryPackingValuesWriter.class);
  }

  @Test
  public void testInt32_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT32,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      DeltaBinaryPackingValuesWriter.class);
  }

  @Test
  public void testInt64() {
    doTestValueWriter(
      PrimitiveTypeName.INT64,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainLongDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testInt64_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT64,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testInt64_V2() {
    doTestValueWriter(
      PrimitiveTypeName.INT64,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainLongDictionaryValuesWriter.class, DeltaBinaryPackingValuesWriterForLong.class);
  }

  @Test
  public void testInt64_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT64,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      DeltaBinaryPackingValuesWriterForLong.class);
  }

  @Test
  public void testInt96() {
    doTestValueWriter(
      PrimitiveTypeName.INT96,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainFixedLenArrayDictionaryValuesWriter.class, FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT96,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_V2() {
    doTestValueWriter(
      PrimitiveTypeName.INT96,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainFixedLenArrayDictionaryValuesWriter.class, FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.INT96,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testDouble() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainDoubleDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testDouble_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testDouble_V2() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainDoubleDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testDouble_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testFloat() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_1_0,
      true,
      false,
      PlainFloatDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testFloat_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_1_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V2() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_2_0,
      true,
      false,
      PlainFloatDictionaryValuesWriter.class, PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V2_NoDict() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_2_0,
      false,
      false,
      PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V1_WithByteStreamSplit() {
   doTestValueWriter(
     PrimitiveTypeName.FLOAT,
     WriterVersion.PARQUET_1_0,
     false,
     true,
     ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V1_WithByteStreamSplit() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_1_0,
      false,
      true,
      ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFloat_V2_WithByteStreamSplit() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_2_0,
      false,
      true,
      ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V2_WithByteStreamSplit() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_2_0,
      false,
      true,
      ByteStreamSplitValuesWriter.class);
  }

  public void testFloat_V1_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_1_0,
      true,
      true,
      PlainFloatDictionaryValuesWriter.class, ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V1_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_1_0,
      true,
      true,
      PlainDoubleDictionaryValuesWriter.class, ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFloat_V2_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
      PrimitiveTypeName.FLOAT,
      WriterVersion.PARQUET_2_0,
      true,
      true,
      PlainFloatDictionaryValuesWriter.class, ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V2_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
      PrimitiveTypeName.DOUBLE,
      WriterVersion.PARQUET_2_0,
      true,
      true,
      PlainDoubleDictionaryValuesWriter.class, ByteStreamSplitValuesWriter.class);
  }

  private void doTestValueWriter(PrimitiveTypeName typeName, WriterVersion version, boolean enableDictionary, boolean enableByteStreamSplit, Class<? extends ValuesWriter> expectedValueWriterClass) {
    ColumnDescriptor mockPath = getMockColumn(typeName);
    ValuesWriterFactory factory = getDefaultFactory(version, enableDictionary, enableByteStreamSplit);
    ValuesWriter writer = factory.newValuesWriter(mockPath);

    validateWriterType(writer, expectedValueWriterClass);
  }

  private void doTestValueWriter(PrimitiveTypeName typeName, WriterVersion version, boolean enableDictionary, boolean enableByteStreamSplit, Class<? extends ValuesWriter> initialValueWriterClass, Class<? extends ValuesWriter> fallbackValueWriterClass) {
    ColumnDescriptor mockPath = getMockColumn(typeName);
    ValuesWriterFactory factory = getDefaultFactory(version, enableDictionary, enableByteStreamSplit);
    ValuesWriter writer = factory.newValuesWriter(mockPath);

    validateFallbackWriter(writer, initialValueWriterClass, fallbackValueWriterClass);
  }

  private ColumnDescriptor getMockColumn(PrimitiveTypeName typeName) {
    ColumnDescriptor mockPath = mock(ColumnDescriptor.class);
    when(mockPath.getType()).thenReturn(typeName);
    return mockPath;
  }

  private ValuesWriterFactory getDefaultFactory(WriterVersion writerVersion, boolean enableDictionary, boolean enableByteStreamSplit) {
    ValuesWriterFactory factory = new DefaultValuesWriterFactory();
    ParquetProperties.builder()
      .withDictionaryEncoding(enableDictionary)
      .withByteStreamSplitEncoding(enableByteStreamSplit)
      .withWriterVersion(writerVersion)
      .withValuesWriterFactory(factory)
      .build();

    return factory;
  }

  private void validateWriterType(ValuesWriter writer, Class<? extends ValuesWriter> valuesWriterClass) {
    assertTrue("Not instance of: " + valuesWriterClass.getName(), valuesWriterClass.isInstance(writer));
  }

  private void validateFallbackWriter(ValuesWriter writer, Class<? extends ValuesWriter> initialWriterClass, Class<? extends ValuesWriter> fallbackWriterClass) {
    validateWriterType(writer, FallbackValuesWriter.class);

    FallbackValuesWriter wr = (FallbackValuesWriter) writer;
    validateWriterType(wr.initialWriter, initialWriterClass);
    validateWriterType(wr.fallBackWriter, fallbackWriterClass);
  }
}
