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
package org.apache.parquet.column.values.pfor;

import static org.junit.Assert.*;

import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.junit.Test;

/**
 * End-to-end tests for PFOR encoding and decoding pipeline.
 * Tests the full writer → serialized bytes → reader round-trip.
 */
public class PforValuesEndToEndTest {

  private static final int DEFAULT_VECTOR_SIZE = PforConstants.DEFAULT_VECTOR_SIZE;

  // ========== INT32 Helper ==========

  private void roundTripInt(int[] values) throws Exception {
    roundTripInt(values, DEFAULT_VECTOR_SIZE);
  }

  private void roundTripInt(int[] values, int vectorSize) throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      int capacity = Math.max(256, values.length * 8);
      writer = new PforValuesWriter.IntPforValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);

      for (int v : values) {
        writer.writeInteger(v);
      }

      assertEquals(Encoding.PFOR, writer.getEncoding());

      BytesInput input = writer.getBytes();
      PforValuesReaderForInt reader = new PforValuesReaderForInt();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        assertEquals("Value mismatch at index " + i, values[i], reader.readInteger());
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== INT64 Helper ==========

  private void roundTripLong(long[] values) throws Exception {
    roundTripLong(values, DEFAULT_VECTOR_SIZE);
  }

  private void roundTripLong(long[] values, int vectorSize) throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      int capacity = Math.max(512, values.length * 16);
      writer = new PforValuesWriter.LongPforValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator(), vectorSize);

      for (long v : values) {
        writer.writeLong(v);
      }

      assertEquals(Encoding.PFOR, writer.getEncoding());

      BytesInput input = writer.getBytes();
      PforValuesReaderForLong reader = new PforValuesReaderForLong();
      reader.initFromPage(values.length, ByteBufferInputStream.wrap(input.toByteBuffer()));

      for (int i = 0; i < values.length; i++) {
        assertEquals("Value mismatch at index " + i, values[i], reader.readLong());
      }
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // ========== INT32 Tests ==========

  @Test
  public void testIntSimpleSequence() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
    }
    roundTripInt(values);
  }

  @Test
  public void testIntAllIdentical() throws Exception {
    int[] values = new int[1024];
    java.util.Arrays.fill(values, 42);
    roundTripInt(values);
  }

  @Test
  public void testIntAllZeros() throws Exception {
    int[] values = new int[1024];
    roundTripInt(values);
  }

  @Test
  public void testIntSingleElement() throws Exception {
    roundTripInt(new int[]{12345});
  }

  @Test
  public void testIntNegativeValues() throws Exception {
    int[] values = {-100, -50, -1, 0, 1, 50, 100};
    roundTripInt(values);
  }

  @Test
  public void testIntMinMaxValues() throws Exception {
    int[] values = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1};
    roundTripInt(values);
  }

  @Test
  public void testIntAlternatingMinMax() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = (i % 2 == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    roundTripInt(values);
  }

  @Test
  public void testIntExactOneVector() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i * 3;
    }
    roundTripInt(values);
  }

  @Test
  public void testIntMultipleVectors() throws Exception {
    int[] values = new int[3000];
    for (int i = 0; i < 3000; i++) {
      values[i] = i * 7 - 10000;
    }
    roundTripInt(values);
  }

  @Test
  public void testIntPartialLastVector() throws Exception {
    // 1025 values = 1 full vector + 1 partial
    int[] values = new int[1025];
    for (int i = 0; i < 1025; i++) {
      values[i] = i;
    }
    roundTripInt(values);
  }

  @Test
  public void testIntWithOutliers() throws Exception {
    // Mostly small values with a few large outliers
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i % 100;
    }
    values[0] = 1_000_000;
    values[512] = -1_000_000;
    values[1023] = Integer.MAX_VALUE;
    roundTripInt(values);
  }

  @Test
  public void testIntLargeRandom() throws Exception {
    Random rng = new Random(42);
    int[] values = new int[10000];
    for (int i = 0; i < 10000; i++) {
      values[i] = rng.nextInt();
    }
    roundTripInt(values);
  }

  @Test
  public void testIntSmallVectorSize() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 11;
    }
    roundTripInt(values, 8); // smallest valid vector size
  }

  @Test
  public void testIntTpcdsLikeDateKeys() throws Exception {
    // Simulate TPC-DS date dimension keys: mostly sequential with a few gaps
    int[] values = new int[2048];
    for (int i = 0; i < 2048; i++) {
      values[i] = 2450815 + i + (i % 100 == 0 ? 100 : 0);
    }
    roundTripInt(values);
  }

  // ========== INT64 Tests ==========

  @Test
  public void testLongSimpleSequence() throws Exception {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
    }
    roundTripLong(values);
  }

  @Test
  public void testLongAllIdentical() throws Exception {
    long[] values = new long[1024];
    java.util.Arrays.fill(values, 999999999999L);
    roundTripLong(values);
  }

  @Test
  public void testLongAllZeros() throws Exception {
    long[] values = new long[1024];
    roundTripLong(values);
  }

  @Test
  public void testLongSingleElement() throws Exception {
    roundTripLong(new long[]{Long.MAX_VALUE});
  }

  @Test
  public void testLongNegativeValues() throws Exception {
    long[] values = {-100_000_000_000L, -1L, 0L, 1L, 100_000_000_000L};
    roundTripLong(values);
  }

  @Test
  public void testLongMinMaxValues() throws Exception {
    long[] values = {Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L};
    roundTripLong(values);
  }

  @Test
  public void testLongMultipleVectors() throws Exception {
    long[] values = new long[3000];
    for (int i = 0; i < 3000; i++) {
      values[i] = (long) i * 1_000_000L - 1_500_000_000L;
    }
    roundTripLong(values);
  }

  @Test
  public void testLongPartialLastVector() throws Exception {
    long[] values = new long[1025];
    for (int i = 0; i < 1025; i++) {
      values[i] = i * 17L;
    }
    roundTripLong(values);
  }

  @Test
  public void testLongWithOutliers() throws Exception {
    long[] values = new long[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i % 100;
    }
    values[0] = Long.MAX_VALUE;
    values[512] = Long.MIN_VALUE;
    values[1023] = 10_000_000_000_000L;
    roundTripLong(values);
  }

  @Test
  public void testLongLargeRandom() throws Exception {
    Random rng = new Random(42);
    long[] values = new long[10000];
    for (int i = 0; i < 10000; i++) {
      values[i] = rng.nextLong();
    }
    roundTripLong(values);
  }

  @Test
  public void testLongSmallVectorSize() throws Exception {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i * 1_000_000L;
    }
    roundTripLong(values, 8);
  }

  // ========== Writer Reset/Reuse ==========

  @Test
  public void testIntWriterReset() throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = new PforValuesWriter.IntPforValuesWriter(
        1024, 1024, new DirectByteBufferAllocator());

    // First batch
    for (int i = 0; i < 100; i++) {
      writer.writeInteger(i);
    }
    BytesInput bytes1 = writer.getBytes();
    assertTrue(bytes1.size() > 0);

    // Reset and second batch
    writer.reset();
    for (int i = 0; i < 50; i++) {
      writer.writeInteger(i * 2);
    }
    BytesInput bytes2 = writer.getBytes();
    assertTrue(bytes2.size() > 0);

    // Verify second batch reads correctly
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(50, ByteBufferInputStream.wrap(bytes2.toByteBuffer()));
    for (int i = 0; i < 50; i++) {
      assertEquals(i * 2, reader.readInteger());
    }

    writer.close();
  }

  @Test
  public void testLongWriterReset() throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = new PforValuesWriter.LongPforValuesWriter(
        1024, 1024, new DirectByteBufferAllocator());

    for (int i = 0; i < 100; i++) {
      writer.writeLong(i * 1000L);
    }
    writer.getBytes();

    writer.reset();
    for (int i = 0; i < 50; i++) {
      writer.writeLong(i * 2000L);
    }
    BytesInput bytes2 = writer.getBytes();

    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(50, ByteBufferInputStream.wrap(bytes2.toByteBuffer()));
    for (int i = 0; i < 50; i++) {
      assertEquals(i * 2000L, reader.readLong());
    }

    writer.close();
  }

  // ========== Reader Skip Tests ==========

  @Test
  public void testIntSkip() throws Exception {
    int[] values = new int[2048];
    for (int i = 0; i < 2048; i++) {
      values[i] = i;
    }

    PforValuesWriter.IntPforValuesWriter writer = new PforValuesWriter.IntPforValuesWriter(
        4096, 4096, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }
    BytesInput bytes = writer.getBytes();

    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(2048, ByteBufferInputStream.wrap(bytes.toByteBuffer()));

    // Skip first 1000
    reader.skip(1000);
    assertEquals(1000, reader.readInteger());
    assertEquals(1001, reader.readInteger());

    // Skip more
    reader.skip(500);
    assertEquals(1502, reader.readInteger());

    writer.close();
  }

  @Test
  public void testLongSkip() throws Exception {
    long[] values = new long[2048];
    for (int i = 0; i < 2048; i++) {
      values[i] = i * 100L;
    }

    PforValuesWriter.LongPforValuesWriter writer = new PforValuesWriter.LongPforValuesWriter(
        4096, 4096, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    BytesInput bytes = writer.getBytes();

    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(2048, ByteBufferInputStream.wrap(bytes.toByteBuffer()));

    reader.skip(1000);
    assertEquals(100000L, reader.readLong());

    writer.close();
  }

  // ========== Empty Input ==========

  @Test
  public void testIntEmptyInput() throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = new PforValuesWriter.IntPforValuesWriter(
        256, 256, new DirectByteBufferAllocator());
    BytesInput bytes = writer.getBytes();
    assertEquals(0, bytes.size());
    writer.close();
  }

  @Test
  public void testLongEmptyInput() throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = new PforValuesWriter.LongPforValuesWriter(
        256, 256, new DirectByteBufferAllocator());
    BytesInput bytes = writer.getBytes();
    assertEquals(0, bytes.size());
    writer.close();
  }
}
