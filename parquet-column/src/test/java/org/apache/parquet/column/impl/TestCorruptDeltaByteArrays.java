/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCorruptDeltaByteArrays {
  @Test
  public void testCorruptDeltaByteArrayVersions() {
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.6.0 (build abcd)", Encoding.DELTA_BYTE_ARRAY));
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads((String) null, Encoding.DELTA_BYTE_ARRAY));
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads((ParsedVersion) null, Encoding.DELTA_BYTE_ARRAY));
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads((SemanticVersion) null, Encoding.DELTA_BYTE_ARRAY));
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.8.0-SNAPSHOT (build abcd)", Encoding.DELTA_BYTE_ARRAY));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.6.0 (build abcd)", Encoding.DELTA_BINARY_PACKED));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads((String) null, Encoding.DELTA_LENGTH_BYTE_ARRAY));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads((ParsedVersion) null, Encoding.PLAIN));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads((SemanticVersion) null, Encoding.RLE));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.8.0-SNAPSHOT (build abcd)", Encoding.RLE_DICTIONARY));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.8.0-SNAPSHOT (build abcd)", Encoding.PLAIN_DICTIONARY));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.8.0-SNAPSHOT (build abcd)", Encoding.BIT_PACKED));
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(
        "parquet-mr version 1.8.0 (build abcd)", Encoding.DELTA_BYTE_ARRAY));
  }

  @Test
  public void testEncodingRequiresSequentialRead() {
    ParsedVersion impala = new ParsedVersion("impala", "1.2.0", "abcd");
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(impala, Encoding.DELTA_BYTE_ARRAY));
    ParsedVersion broken = new ParsedVersion("parquet-mr", "1.8.0-SNAPSHOT", "abcd");
    assertTrue(CorruptDeltaByteArrays.requiresSequentialReads(broken, Encoding.DELTA_BYTE_ARRAY));
    ParsedVersion fixed = new ParsedVersion("parquet-mr", "1.8.0", "abcd");
    assertFalse(CorruptDeltaByteArrays.requiresSequentialReads(fixed, Encoding.DELTA_BYTE_ARRAY));
  }

  private DeltaByteArrayWriter getDeltaByteArrayWriter() {
    return new DeltaByteArrayWriter(10, 100, new HeapByteBufferAllocator());
  }

  @Test
  public void testReassemblyWithCorruptPage() throws Exception {
    DeltaByteArrayWriter writer = getDeltaByteArrayWriter();

    String lastValue = null;
    for (int i = 0; i < 10; i += 1) {
      lastValue = str(i);
      writer.writeBytes(Binary.fromString(lastValue));
    }
    ByteBuffer firstPageBytes = writer.getBytes().toByteBuffer();

    writer.reset(); // sets previous to new byte[0]
    corruptWriter(writer, lastValue);

    for (int i = 10; i < 20; i += 1) {
      writer.writeBytes(Binary.fromString(str(i)));
    }
    ByteBuffer corruptPageBytes = writer.getBytes().toByteBuffer();

    DeltaByteArrayReader firstPageReader = new DeltaByteArrayReader();
    firstPageReader.initFromPage(10, ByteBufferInputStream.wrap(firstPageBytes));
    for (int i = 0; i < 10; i += 1) {
      assertEquals(str(i), firstPageReader.readBytes().toStringUsingUTF8());
    }

    DeltaByteArrayReader corruptPageReader = new DeltaByteArrayReader();
    corruptPageReader.initFromPage(10, ByteBufferInputStream.wrap(corruptPageBytes));
    try {
      corruptPageReader.readBytes();
      fail("Corrupt page did not throw an exception when read");
    } catch (ArrayIndexOutOfBoundsException e) {
      // expected, this is a corrupt page
    }

    DeltaByteArrayReader secondPageReader = new DeltaByteArrayReader();
    secondPageReader.initFromPage(10, ByteBufferInputStream.wrap(corruptPageBytes));
    secondPageReader.setPreviousReader(firstPageReader);

    for (int i = 10; i < 20; i += 1) {
      assertEquals(secondPageReader.readBytes().toStringUsingUTF8(), str(i));
    }
  }

  @Test
  public void testReassemblyWithoutCorruption() throws Exception {
    DeltaByteArrayWriter writer = getDeltaByteArrayWriter();

    for (int i = 0; i < 10; i += 1) {
      writer.writeBytes(Binary.fromString(str(i)));
    }
    ByteBuffer firstPageBytes = writer.getBytes().toByteBuffer();

    writer.reset(); // sets previous to new byte[0]

    for (int i = 10; i < 20; i += 1) {
      writer.writeBytes(Binary.fromString(str(i)));
    }
    ByteBuffer secondPageBytes = writer.getBytes().toByteBuffer();

    DeltaByteArrayReader firstPageReader = new DeltaByteArrayReader();
    firstPageReader.initFromPage(10, ByteBufferInputStream.wrap(firstPageBytes));
    for (int i = 0; i < 10; i += 1) {
      assertEquals(firstPageReader.readBytes().toStringUsingUTF8(), str(i));
    }

    DeltaByteArrayReader secondPageReader = new DeltaByteArrayReader();
    secondPageReader.initFromPage(10, ByteBufferInputStream.wrap(secondPageBytes));
    secondPageReader.setPreviousReader(firstPageReader);

    for (int i = 10; i < 20; i += 1) {
      assertEquals(secondPageReader.readBytes().toStringUsingUTF8(), str(i));
    }
  }

  @Test
  public void testOldReassemblyWithoutCorruption() throws Exception {
    DeltaByteArrayWriter writer = getDeltaByteArrayWriter();

    for (int i = 0; i < 10; i += 1) {
      writer.writeBytes(Binary.fromString(str(i)));
    }
    ByteBuffer firstPageBytes = writer.getBytes().toByteBuffer();

    writer.reset(); // sets previous to new byte[0]

    for (int i = 10; i < 20; i += 1) {
      writer.writeBytes(Binary.fromString(str(i)));
    }
    ByteBuffer secondPageBytes = writer.getBytes().toByteBuffer();

    DeltaByteArrayReader firstPageReader = new DeltaByteArrayReader();
    firstPageReader.initFromPage(10, ByteBufferInputStream.wrap(firstPageBytes));
    for (int i = 0; i < 10; i += 1) {
      assertEquals(firstPageReader.readBytes().toStringUsingUTF8(), str(i));
    }

    DeltaByteArrayReader secondPageReader = new DeltaByteArrayReader();
    secondPageReader.initFromPage(10, ByteBufferInputStream.wrap(secondPageBytes));

    for (int i = 10; i < 20; i += 1) {
      assertEquals(secondPageReader.readBytes().toStringUsingUTF8(), str(i));
    }
  }

  @Test
  public void testColumnReaderImplWithCorruptPage() throws Exception {
    ColumnDescriptor column =
        new ColumnDescriptor(new String[] {"s"}, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
    MemPageStore pages = new MemPageStore(0);
    PageWriter memWriter = pages.getPageWriter(column);

    ParquetProperties parquetProps =
        ParquetProperties.builder().withDictionaryEncoding(false).build();

    // get generic repetition and definition level bytes to use for pages
    ValuesWriter rdValues = parquetProps.newDefinitionLevelWriter(column);
    for (int i = 0; i < 10; i += 1) {
      rdValues.writeInteger(0);
    }
    // use a byte array backed BytesInput because it is reused
    BytesInput rd = BytesInput.from(rdValues.getBytes().toByteArray());
    DeltaByteArrayWriter writer = getDeltaByteArrayWriter();
    String lastValue = null;
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 10; i += 1) {
      lastValue = str(i);
      writer.writeBytes(Binary.fromString(lastValue));
      values.add(lastValue);
    }

    memWriter.writePage(
        BytesInput.concat(rd, rd, writer.getBytes()),
        10, /* number of values in the page */
        new BinaryStatistics(),
        rdValues.getEncoding(),
        rdValues.getEncoding(),
        writer.getEncoding());
    pages.addRowCount(10);

    writer.reset(); // sets previous to new byte[0]
    corruptWriter(writer, lastValue);
    for (int i = 10; i < 20; i += 1) {
      String value = str(i);
      writer.writeBytes(Binary.fromString(value));
      values.add(value);
    }

    memWriter.writePage(
        BytesInput.concat(rd, rd, writer.getBytes()),
        10, /* number of values in the page */
        new BinaryStatistics(),
        rdValues.getEncoding(),
        rdValues.getEncoding(),
        writer.getEncoding());
    pages.addRowCount(10);

    final List<String> actualValues = new ArrayList<>();
    PrimitiveConverter converter = new PrimitiveConverter() {
      @Override
      public void addBinary(Binary value) {
        actualValues.add(value.toStringUsingUTF8());
      }
    };

    ColumnReaderImpl columnReader = new ColumnReaderImpl(
        column, pages.getPageReader(column), converter, new ParsedVersion("parquet-mr", "1.6.0", "abcd"));

    while (actualValues.size() < columnReader.getTotalValueCount()) {
      columnReader.writeCurrentValueToConverter();
      columnReader.consume();
    }

    Assert.assertEquals(values, actualValues);
  }

  @Test
  public void testPreviousReaderSetting() {
    Binary previous = Binary.fromString("<<<PREVIOUS>>>");
    DeltaByteArrayReader previousReader = new DeltaByteArrayReader();
    setPrevious(previousReader, previous);

    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    reader.setPreviousReader(previousReader);
    assertSame(previous, getPrevious(reader));

    reader.setPreviousReader(null);
    assertSame("The previous field should have not changed", previous, getPrevious(reader));

    reader.setPreviousReader(Mockito.mock(DictionaryValuesReader.class));
    assertSame("The previous field should have not changed", previous, getPrevious(reader));
  }

  private Binary getPrevious(DeltaByteArrayReader reader) {
    try {
      Field previousField = reader.getClass().getDeclaredField("previous");
      previousField.setAccessible(true);
      return (Binary) previousField.get(reader);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new AssertionError("Unable to get the private field \"previous\" of the reader" + reader, e);
    }
  }

  private void setPrevious(DeltaByteArrayReader reader, Binary previous) {
    try {
      Field previousField = reader.getClass().getDeclaredField("previous");
      previousField.setAccessible(true);
      previousField.set(reader, previous);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new AssertionError("Unable to set the private field \"previous\" of the reader" + reader, e);
    }
  }

  public void corruptWriter(DeltaByteArrayWriter writer, String data) throws Exception {
    Field previous = writer.getClass().getDeclaredField("previous");
    previous.setAccessible(true);
    previous.set(writer, Binary.fromString(data).getBytesUnsafe());
  }

  public String str(int i) {
    char c = 'a';
    return "aaaaaaaaaaa" + (char) (c + i);
  }
}
