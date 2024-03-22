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
package org.apache.parquet.column.values.dictionary;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestDictionary {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new DirectByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  private <I extends DictionaryValuesWriter> FallbackValuesWriter<I, PlainValuesWriter> plainFallBack(
      I dvw, int initialSize) {
    return FallbackValuesWriter.of(dvw, new PlainValuesWriter(initialSize, initialSize * 5, allocator));
  }

  private FallbackValuesWriter<PlainBinaryDictionaryValuesWriter, PlainValuesWriter>
      newPlainBinaryDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    return plainFallBack(
        new PlainBinaryDictionaryValuesWriter(
            maxDictionaryByteSize, PLAIN_DICTIONARY, PLAIN_DICTIONARY, allocator),
        initialSize);
  }

  private FallbackValuesWriter<PlainLongDictionaryValuesWriter, PlainValuesWriter> newPlainLongDictionaryValuesWriter(
      int maxDictionaryByteSize, int initialSize) {
    return plainFallBack(
        new PlainLongDictionaryValuesWriter(
            maxDictionaryByteSize, PLAIN_DICTIONARY, PLAIN_DICTIONARY, allocator),
        initialSize);
  }

  private FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter>
      newPlainIntegerDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    return plainFallBack(
        new PlainIntegerDictionaryValuesWriter(
            maxDictionaryByteSize, PLAIN_DICTIONARY, PLAIN_DICTIONARY, allocator),
        initialSize);
  }

  private FallbackValuesWriter<PlainDoubleDictionaryValuesWriter, PlainValuesWriter>
      newPlainDoubleDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    return plainFallBack(
        new PlainDoubleDictionaryValuesWriter(
            maxDictionaryByteSize, PLAIN_DICTIONARY, PLAIN_DICTIONARY, allocator),
        initialSize);
  }

  private FallbackValuesWriter<PlainFloatDictionaryValuesWriter, PlainValuesWriter>
      newPlainFloatDictionaryValuesWriter(int maxDictionaryByteSize, int initialSize) {
    return plainFallBack(
        new PlainFloatDictionaryValuesWriter(
            maxDictionaryByteSize, PLAIN_DICTIONARY, PLAIN_DICTIONARY, allocator),
        initialSize);
  }

  @Test
  public void testBinaryDictionary() throws IOException {
    int COUNT = 100;
    try (ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(200, 10000)) {
      writeRepeated(COUNT, cw, "a");
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      writeRepeated(COUNT, cw, "b");
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      // now we will fall back
      writeDistinct(COUNT, cw, "c");
      BytesInput bytes3 = getBytesAndCheckEncoding(cw, PLAIN);

      DictionaryValuesReader cr = initDicReader(cw, BINARY);
      checkRepeated(COUNT, bytes1, cr, "a");
      checkRepeated(COUNT, bytes2, cr, "b");
      BinaryPlainValuesReader cr2 = new BinaryPlainValuesReader();
      checkDistinct(COUNT, bytes3, cr2, "c");
    }
  }

  @Test
  public void testSkipInBinaryDictionary() throws Exception {
    try (ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(1000, 10000)) {
      writeRepeated(100, cw, "a");
      writeDistinct(100, cw, "b");
      assertEquals(PLAIN_DICTIONARY, cw.getEncoding());

      // Test skip and skip-n with dictionary encoding
      ByteBufferInputStream stream = cw.getBytes().toInputStream();
      DictionaryValuesReader cr = initDicReader(cw, BINARY);
      cr.initFromPage(200, stream);
      for (int i = 0; i < 100; i += 2) {
        assertEquals(Binary.fromString("a" + i % 10), cr.readBytes());
        cr.skip();
      }
      int skipCount;
      for (int i = 0; i < 100; i += skipCount + 1) {
        skipCount = (100 - i) / 2;
        assertEquals(Binary.fromString("b" + i), cr.readBytes());
        cr.skip(skipCount);
      }

      // Ensure fallback
      writeDistinct(1000, cw, "c");
      assertEquals(PLAIN, cw.getEncoding());

      // Test skip and skip-n with plain encoding (after fallback)
      ValuesReader plainReader = new BinaryPlainValuesReader();
      plainReader.initFromPage(1200, cw.getBytes().toInputStream());
      plainReader.skip(200);
      for (int i = 0; i < 100; i += 2) {
        assertEquals("c" + i, plainReader.readBytes().toStringUsingUTF8());
        plainReader.skip();
      }
      for (int i = 100; i < 1000; i += skipCount + 1) {
        skipCount = (1000 - i) / 2;
        assertEquals(Binary.fromString("c" + i), plainReader.readBytes());
        plainReader.skip(skipCount);
      }
    }
  }

  @Test
  public void testBinaryDictionaryFallBack() throws IOException {
    int slabSize = 100;
    int maxDictionaryByteSize = 50;
    try (final ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(maxDictionaryByteSize, slabSize)) {
      int fallBackThreshold = maxDictionaryByteSize;
      int dataSize = 0;
      for (long i = 0; i < 100; i++) {
        Binary binary = Binary.fromString("str" + i);
        cw.writeBytes(binary);
        dataSize += (binary.length() + 4);
        if (dataSize < fallBackThreshold) {
          assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
        } else {
          assertEquals(PLAIN, cw.getEncoding());
        }
      }

      // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
      ValuesReader reader = new BinaryPlainValuesReader();
      reader.initFromPage(100, cw.getBytes().toInputStream());

      for (long i = 0; i < 100; i++) {
        assertEquals(Binary.fromString("str" + i), reader.readBytes());
      }

      // simulate cutting the page
      cw.reset();
      assertEquals(0, cw.getBufferedSize());
    }
  }

  @Test
  public void testBinaryDictionaryIntegerOverflow() {
    Binary mock = Mockito.mock(Binary.class);
    Mockito.when(mock.length()).thenReturn(Integer.MAX_VALUE - 1);
    // make the writer happy
    Mockito.when(mock.copy()).thenReturn(Binary.fromString(" world"));

    try (final ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(100, 100)) {
      cw.writeBytes(Binary.fromString("hello"));
      cw.writeBytes(mock);

      assertEquals(PLAIN, cw.getEncoding());
    }
  }

  @Test
  public void testBinaryDictionaryChangedValues() throws IOException {
    int COUNT = 100;
    try (ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(200, 10000)) {
      writeRepeatedWithReuse(COUNT, cw, "a");
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      writeRepeatedWithReuse(COUNT, cw, "b");
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      // now we will fall back
      writeDistinct(COUNT, cw, "c");
      BytesInput bytes3 = getBytesAndCheckEncoding(cw, PLAIN);

      DictionaryValuesReader cr = initDicReader(cw, BINARY);
      checkRepeated(COUNT, bytes1, cr, "a");
      checkRepeated(COUNT, bytes2, cr, "b");
      BinaryPlainValuesReader cr2 = new BinaryPlainValuesReader();
      checkDistinct(COUNT, bytes3, cr2, "c");
    }
  }

  @Test
  public void testFirstPageFallBack() throws IOException {
    int COUNT = 1000;
    try (ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(10000, 10000)) {
      writeDistinct(COUNT, cw, "a");
      // not efficient so falls back
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN);
      writeRepeated(COUNT, cw, "b");
      // still plain because we fell back on first page
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN);

      ValuesReader cr = new BinaryPlainValuesReader();
      checkDistinct(COUNT, bytes1, cr, "a");
      checkRepeated(COUNT, bytes2, cr, "b");
    }
  }

  @Test
  public void testSecondPageFallBack() throws IOException {
    int COUNT = 1000;
    try (ValuesWriter cw = newPlainBinaryDictionaryValuesWriter(1000, 10000)) {
      writeRepeated(COUNT, cw, "a");
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      writeDistinct(COUNT, cw, "b");
      // not efficient so falls back
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN);
      writeRepeated(COUNT, cw, "a");
      // still plain because we fell back on previous page
      BytesInput bytes3 = getBytesAndCheckEncoding(cw, PLAIN);

      ValuesReader cr = initDicReader(cw, BINARY);
      checkRepeated(COUNT, bytes1, cr, "a");
      cr = new BinaryPlainValuesReader();
      checkDistinct(COUNT, bytes2, cr, "b");
      checkRepeated(COUNT, bytes3, cr, "a");
    }
  }

  @Test
  public void testLongDictionary() throws IOException {
    int COUNT = 1000;
    int COUNT2 = 2000;
    try (final FallbackValuesWriter<PlainLongDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainLongDictionaryValuesWriter(10000, 10000)) {
      for (long i = 0; i < COUNT; i++) {
        cw.writeLong(i % 50);
      }
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      for (long i = COUNT2; i > 0; i--) {
        cw.writeLong(i % 50);
      }
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      DictionaryValuesReader cr = initDicReader(cw, PrimitiveTypeName.INT64);

      cr.initFromPage(COUNT, bytes1.toInputStream());
      for (long i = 0; i < COUNT; i++) {
        long back = cr.readLong();
        assertEquals(i % 50, back);
      }

      cr.initFromPage(COUNT2, bytes2.toInputStream());
      for (long i = COUNT2; i > 0; i--) {
        long back = cr.readLong();
        assertEquals(i % 50, back);
      }
    }
  }

  private void roundTripLong(
      FallbackValuesWriter<PlainLongDictionaryValuesWriter, PlainValuesWriter> cw,
      ValuesReader reader,
      int maxDictionaryByteSize)
      throws IOException {
    int fallBackThreshold = maxDictionaryByteSize / 8;
    for (long i = 0; i < 100; i++) {
      cw.writeLong(i);
      if (i < fallBackThreshold) {
        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
      } else {
        assertEquals(cw.getEncoding(), PLAIN);
      }
    }

    reader.initFromPage(100, cw.getBytes().toInputStream());

    for (long i = 0; i < 100; i++) {
      assertEquals(i, reader.readLong());
    }

    // Test skip with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    for (int i = 0; i < 100; i += 2) {
      assertEquals(i, reader.readLong());
      reader.skip();
    }

    // Test skip-n with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < 100; i += skipCount + 1) {
      skipCount = (100 - i) / 2;
      assertEquals(i, reader.readLong());
      reader.skip(skipCount);
    }
  }

  @Test
  public void testLongDictionaryFallBack() throws IOException {
    int slabSize = 100;
    int maxDictionaryByteSize = 50;
    try (final FallbackValuesWriter<PlainLongDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainLongDictionaryValuesWriter(maxDictionaryByteSize, slabSize)) {
      // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
      ValuesReader reader = new PlainValuesReader.LongPlainValuesReader();

      roundTripLong(cw, reader, maxDictionaryByteSize);
      // simulate cutting the page
      cw.reset();
      assertEquals(0, cw.getBufferedSize());
      cw.resetDictionary();

      roundTripLong(cw, reader, maxDictionaryByteSize);
    }
  }

  @Test
  public void testDoubleDictionary() throws IOException {

    int COUNT = 1000;
    int COUNT2 = 2000;
    try (final FallbackValuesWriter<PlainDoubleDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainDoubleDictionaryValuesWriter(10000, 10000)) {

      for (double i = 0; i < COUNT; i++) {
        cw.writeDouble(i % 50);
      }

      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      for (double i = COUNT2; i > 0; i--) {
        cw.writeDouble(i % 50);
      }
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      final DictionaryValuesReader cr = initDicReader(cw, DOUBLE);

      cr.initFromPage(COUNT, bytes1.toInputStream());
      for (double i = 0; i < COUNT; i++) {
        double back = cr.readDouble();
        assertEquals(i % 50, back, 0.0);
      }

      cr.initFromPage(COUNT2, bytes2.toInputStream());
      for (double i = COUNT2; i > 0; i--) {
        double back = cr.readDouble();
        assertEquals(i % 50, back, 0.0);
      }
    }
  }

  private void roundTripDouble(
      FallbackValuesWriter<PlainDoubleDictionaryValuesWriter, PlainValuesWriter> cw,
      ValuesReader reader,
      int maxDictionaryByteSize)
      throws IOException {
    int fallBackThreshold = maxDictionaryByteSize / 8;
    for (double i = 0; i < 100; i++) {
      cw.writeDouble(i);
      if (i < fallBackThreshold) {
        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
      } else {
        assertEquals(cw.getEncoding(), PLAIN);
      }
    }

    reader.initFromPage(100, cw.getBytes().toInputStream());

    for (double i = 0; i < 100; i++) {
      assertEquals(i, reader.readDouble(), 0.00001);
    }

    // Test skip with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    for (int i = 0; i < 100; i += 2) {
      assertEquals(i, reader.readDouble(), 0.0);
      reader.skip();
    }

    // Test skip-n with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < 100; i += skipCount + 1) {
      skipCount = (100 - i) / 2;
      assertEquals(i, reader.readDouble(), 0.0);
      reader.skip(skipCount);
    }
  }

  @Test
  public void testDoubleDictionaryFallBack() throws IOException {
    int slabSize = 100;
    int maxDictionaryByteSize = 50;
    try (final FallbackValuesWriter<PlainDoubleDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainDoubleDictionaryValuesWriter(maxDictionaryByteSize, slabSize)) {

      // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
      ValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();

      roundTripDouble(cw, reader, maxDictionaryByteSize);
      // simulate cutting the page
      cw.reset();
      assertEquals(0, cw.getBufferedSize());
      cw.resetDictionary();

      roundTripDouble(cw, reader, maxDictionaryByteSize);
    }
  }

  @Test
  public void testIntDictionary() throws IOException {

    int COUNT = 2000;
    int COUNT2 = 4000;
    try (final FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainIntegerDictionaryValuesWriter(10000, 10000)) {

      for (int i = 0; i < COUNT; i++) {
        cw.writeInteger(i % 50);
      }
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      for (int i = COUNT2; i > 0; i--) {
        cw.writeInteger(i % 50);
      }
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      DictionaryValuesReader cr = initDicReader(cw, INT32);

      cr.initFromPage(COUNT, bytes1.toInputStream());
      for (int i = 0; i < COUNT; i++) {
        int back = cr.readInteger();
        assertEquals(i % 50, back);
      }

      cr.initFromPage(COUNT2, bytes2.toInputStream());
      for (int i = COUNT2; i > 0; i--) {
        int back = cr.readInteger();
        assertEquals(i % 50, back);
      }
    }
  }

  private void roundTripInt(
      FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> cw,
      ValuesReader reader,
      int maxDictionaryByteSize)
      throws IOException {
    int fallBackThreshold = maxDictionaryByteSize / 4;
    for (int i = 0; i < 100; i++) {
      cw.writeInteger(i);
      if (i < fallBackThreshold) {
        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
      } else {
        assertEquals(cw.getEncoding(), PLAIN);
      }
    }

    reader.initFromPage(100, cw.getBytes().toInputStream());

    for (int i = 0; i < 100; i++) {
      assertEquals(i, reader.readInteger());
    }

    // Test skip with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    for (int i = 0; i < 100; i += 2) {
      assertEquals(i, reader.readInteger());
      reader.skip();
    }

    // Test skip-n with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < 100; i += skipCount + 1) {
      skipCount = (100 - i) / 2;
      assertEquals(i, reader.readInteger());
      reader.skip(skipCount);
    }
  }

  @Test
  public void testIntDictionaryFallBack() throws IOException {
    int slabSize = 100;
    int maxDictionaryByteSize = 50;
    try (final FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainIntegerDictionaryValuesWriter(maxDictionaryByteSize, slabSize)) {

      // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
      ValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();

      roundTripInt(cw, reader, maxDictionaryByteSize);
      // simulate cutting the page
      cw.reset();
      assertEquals(0, cw.getBufferedSize());
      cw.resetDictionary();

      roundTripInt(cw, reader, maxDictionaryByteSize);
    }
  }

  @Test
  public void testFloatDictionary() throws IOException {

    int COUNT = 2000;
    int COUNT2 = 4000;
    try (final FallbackValuesWriter<PlainFloatDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainFloatDictionaryValuesWriter(10000, 10000)) {

      for (float i = 0; i < COUNT; i++) {
        cw.writeFloat(i % 50);
      }
      BytesInput bytes1 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      for (float i = COUNT2; i > 0; i--) {
        cw.writeFloat(i % 50);
      }
      BytesInput bytes2 = getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      assertEquals(50, cw.initialWriter.getDictionarySize());

      DictionaryValuesReader cr = initDicReader(cw, FLOAT);

      cr.initFromPage(COUNT, bytes1.toInputStream());
      for (float i = 0; i < COUNT; i++) {
        float back = cr.readFloat();
        assertEquals(i % 50, back, 0.0f);
      }

      cr.initFromPage(COUNT2, bytes2.toInputStream());
      for (float i = COUNT2; i > 0; i--) {
        float back = cr.readFloat();
        assertEquals(i % 50, back, 0.0f);
      }
    }
  }

  private void roundTripFloat(
      FallbackValuesWriter<PlainFloatDictionaryValuesWriter, PlainValuesWriter> cw,
      ValuesReader reader,
      int maxDictionaryByteSize)
      throws IOException {
    int fallBackThreshold = maxDictionaryByteSize / 4;
    for (float i = 0; i < 100; i++) {
      cw.writeFloat(i);
      if (i < fallBackThreshold) {
        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
      } else {
        assertEquals(cw.getEncoding(), PLAIN);
      }
    }

    reader.initFromPage(100, cw.getBytes().toInputStream());

    for (float i = 0; i < 100; i++) {
      assertEquals(i, reader.readFloat(), 0.00001);
    }

    // Test skip with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    for (int i = 0; i < 100; i += 2) {
      assertEquals(i, reader.readFloat(), 0.0f);
      reader.skip();
    }

    // Test skip-n with plain encoding
    reader.initFromPage(100, cw.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < 100; i += skipCount + 1) {
      skipCount = (100 - i) / 2;
      assertEquals(i, reader.readFloat(), 0.0f);
      reader.skip(skipCount);
    }
  }

  @Test
  public void testFloatDictionaryFallBack() throws IOException {
    int slabSize = 100;
    int maxDictionaryByteSize = 50;
    try (final FallbackValuesWriter<PlainFloatDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainFloatDictionaryValuesWriter(maxDictionaryByteSize, slabSize)) {

      // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
      ValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();

      roundTripFloat(cw, reader, maxDictionaryByteSize);
      // simulate cutting the page
      cw.reset();
      assertEquals(0, cw.getBufferedSize());
      cw.resetDictionary();

      roundTripFloat(cw, reader, maxDictionaryByteSize);
    }
  }

  @Test
  public void testZeroValues() throws IOException {
    try (FallbackValuesWriter<PlainIntegerDictionaryValuesWriter, PlainValuesWriter> cw =
        newPlainIntegerDictionaryValuesWriter(100, 100)) {
      cw.writeInteger(34);
      cw.writeInteger(34);
      getBytesAndCheckEncoding(cw, PLAIN_DICTIONARY);
      DictionaryValuesReader reader = initDicReader(cw, INT32);

      // pretend there are 100 nulls. what matters is offset = bytes.length.
      ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0x00, 0x01, 0x02, 0x03}); // data doesn't matter
      ByteBufferInputStream stream = ByteBufferInputStream.wrap(bytes);
      stream.skipFully(stream.available());
      reader.initFromPage(100, stream);

      // Testing the deprecated behavior of using byte arrays directly
      reader = initDicReader(cw, INT32);
      int offset = bytes.remaining();
      reader.initFromPage(100, bytes, offset);
    }
  }

  private DictionaryValuesReader initDicReader(ValuesWriter cw, PrimitiveTypeName type) throws IOException {
    final DictionaryPage dictionaryPage = cw.toDictPageAndClose().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, type, 0, 0);
    final Dictionary dictionary = PLAIN.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);
    return cr;
  }

  private void checkDistinct(int COUNT, BytesInput bytes, ValuesReader cr, String prefix) throws IOException {
    cr.initFromPage(COUNT, bytes.toInputStream());
    for (int i = 0; i < COUNT; i++) {
      Assert.assertEquals(prefix + i, cr.readBytes().toStringUsingUTF8());
    }
  }

  private void checkRepeated(int COUNT, BytesInput bytes, ValuesReader cr, String prefix) throws IOException {
    cr.initFromPage(COUNT, bytes.toInputStream());
    for (int i = 0; i < COUNT; i++) {
      Assert.assertEquals(prefix + i % 10, cr.readBytes().toStringUsingUTF8());
    }
  }

  private void writeDistinct(int COUNT, ValuesWriter cw, String prefix) {
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString(prefix + i));
    }
  }

  private void writeRepeated(int COUNT, ValuesWriter cw, String prefix) {
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString(prefix + i % 10));
    }
  }

  private void writeRepeatedWithReuse(int COUNT, ValuesWriter cw, String prefix) {
    Binary reused = Binary.fromReusedByteArray((prefix + "0").getBytes(StandardCharsets.UTF_8));
    for (int i = 0; i < COUNT; i++) {
      Binary content = Binary.fromString(prefix + i % 10);
      System.arraycopy(content.getBytesUnsafe(), 0, reused.getBytesUnsafe(), 0, reused.length());
      cw.writeBytes(reused);
    }
  }

  private BytesInput getBytesAndCheckEncoding(ValuesWriter cw, Encoding encoding) throws IOException {
    BytesInput bytes = BytesInput.copy(cw.getBytes());
    assertEquals(encoding, cw.getEncoding());
    cw.reset();
    return bytes;
  }
}
