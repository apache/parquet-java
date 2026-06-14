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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.Test;

/**
 * Tests focused on PFOR bit-packing correctness across different bit widths
 * and vector sizes.
 */
public class PforBitPackingTest {

  // Round-trip helper that verifies bit-packing for int values in a given range
  private void verifyIntRoundTrip(int[] values) throws Exception {
    int capacity = Math.max(256, values.length * 8);
    PforValuesWriter.IntPforValuesWriter writer = new PforValuesWriter.IntPforValuesWriter(
        capacity, capacity, new DirectByteBufferAllocator());

    for (int v : values) {
      writer.writeInteger(v);
    }

    BytesInput bytes = writer.getBytes();
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(bytes.toByteBuffer()));

    for (int i = 0; i < values.length; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readInteger());
    }

    writer.close();
  }

  private void verifyLongRoundTrip(long[] values) throws Exception {
    int capacity = Math.max(512, values.length * 16);
    PforValuesWriter.LongPforValuesWriter writer = new PforValuesWriter.LongPforValuesWriter(
        capacity, capacity, new DirectByteBufferAllocator());

    for (long v : values) {
      writer.writeLong(v);
    }

    BytesInput bytes = writer.getBytes();
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(bytes.toByteBuffer()));

    for (int i = 0; i < values.length; i++) {
      assertEquals("Mismatch at index " + i, values[i], reader.readLong());
    }

    writer.close();
  }

  // ========== INT32 Bit Width Coverage ==========

  @Test
  public void testIntBitWidth0() throws Exception {
    // All same value → bitWidth=0, no packed bytes
    int[] values = new int[100];
    java.util.Arrays.fill(values, 777);
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntBitWidth1() throws Exception {
    // Values: base + {0, 1}
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 1000 + (i % 2);
    }
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntBitWidth8() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = 5000 + (i % 256);
    }
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntBitWidth16() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i;
    }
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntBitWidth32() throws Exception {
    // Full range int values
    int[] values = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE,
        0x7FFFFFFF, 0x40000000, -2147483648};
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntPartialGroup() throws Exception {
    // 13 values: 1 full group of 8 + 5 remaining
    int[] values = new int[13];
    for (int i = 0; i < 13; i++) {
      values[i] = i * 100;
    }
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntExactlyOneGroup() throws Exception {
    // Exactly 8 values
    int[] values = {10, 20, 30, 40, 50, 60, 70, 80};
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntSevenValues() throws Exception {
    // Less than one full group
    int[] values = {1, 2, 3, 4, 5, 6, 7};
    verifyIntRoundTrip(values);
  }

  // ========== INT64 Bit Width Coverage ==========

  @Test
  public void testLongBitWidth0() throws Exception {
    long[] values = new long[100];
    java.util.Arrays.fill(values, 123456789L);
    verifyLongRoundTrip(values);
  }

  @Test
  public void testLongBitWidth1() throws Exception {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = 1_000_000L + (i % 2);
    }
    verifyLongRoundTrip(values);
  }

  @Test
  public void testLongBitWidth32() throws Exception {
    long[] values = new long[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = (long) i * 1_000_000L;
    }
    verifyLongRoundTrip(values);
  }

  @Test
  public void testLongBitWidth64() throws Exception {
    long[] values = {Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L};
    verifyLongRoundTrip(values);
  }

  @Test
  public void testLongPartialGroup() throws Exception {
    long[] values = new long[13];
    for (int i = 0; i < 13; i++) {
      values[i] = i * 100_000L;
    }
    verifyLongRoundTrip(values);
  }

  // ========== Page Header Verification ==========

  @Test
  public void testIntPageHeaderFormat() throws Exception {
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
    }

    PforValuesWriter.IntPforValuesWriter writer = new PforValuesWriter.IntPforValuesWriter(
        1024, 1024, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }

    ByteBuffer buf = writer.getBytes().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);

    // Verify header
    assertEquals("packing_mode", 0, buf.get(0) & 0xFF);
    assertEquals("log_vector_size", 10, buf.get(1) & 0xFF);
    assertEquals("value_byte_width", 4, buf.get(2) & 0xFF);

    buf.position(3);
    assertEquals("num_elements", 100, buf.getInt());

    writer.close();
  }

  @Test
  public void testLongPageHeaderFormat() throws Exception {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i;
    }

    PforValuesWriter.LongPforValuesWriter writer = new PforValuesWriter.LongPforValuesWriter(
        1024, 1024, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }

    ByteBuffer buf = writer.getBytes().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);

    assertEquals("packing_mode", 0, buf.get(0) & 0xFF);
    assertEquals("log_vector_size", 10, buf.get(1) & 0xFF);
    assertEquals("value_byte_width", 8, buf.get(2) & 0xFF);

    buf.position(3);
    assertEquals("num_elements", 100, buf.getInt());

    writer.close();
  }

  // ========== Exception Handling ==========

  @Test
  public void testIntManyExceptions() throws Exception {
    // More than half are outliers — cost model should widen bit width
    int[] values = new int[100];
    for (int i = 0; i < 100; i++) {
      values[i] = (i % 2 == 0) ? i : 1_000_000 + i;
    }
    verifyIntRoundTrip(values);
  }

  @Test
  public void testIntAllExceptionsScenario() throws Exception {
    // Very wide range but few values: might make everything exceptions or wide pack
    int[] values = {0, Integer.MAX_VALUE, Integer.MIN_VALUE, 42, -42};
    verifyIntRoundTrip(values);
  }

  @Test
  public void testLongManyExceptions() throws Exception {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = (i % 2 == 0) ? i : Long.MAX_VALUE - i;
    }
    verifyLongRoundTrip(values);
  }
}
