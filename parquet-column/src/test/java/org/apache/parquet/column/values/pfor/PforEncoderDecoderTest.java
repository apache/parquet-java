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

import org.junit.Test;

/**
 * Tests for PFOR cost model and bit width utilities.
 */
public class PforEncoderDecoderTest {

  // ========== Bit Width Tests ==========

  @Test
  public void testBitWidthForInt() {
    assertEquals(0, PforEncoderDecoder.bitWidthForInt(0));
    assertEquals(1, PforEncoderDecoder.bitWidthForInt(1));
    assertEquals(2, PforEncoderDecoder.bitWidthForInt(2));
    assertEquals(2, PforEncoderDecoder.bitWidthForInt(3));
    assertEquals(3, PforEncoderDecoder.bitWidthForInt(4));
    assertEquals(8, PforEncoderDecoder.bitWidthForInt(255));
    assertEquals(9, PforEncoderDecoder.bitWidthForInt(256));
    assertEquals(16, PforEncoderDecoder.bitWidthForInt(65535));
    assertEquals(31, PforEncoderDecoder.bitWidthForInt(Integer.MAX_VALUE));
    // Unsigned: -1 == 0xFFFFFFFF → 32 bits
    assertEquals(32, PforEncoderDecoder.bitWidthForInt(-1));
  }

  @Test
  public void testBitWidthForLong() {
    assertEquals(0, PforEncoderDecoder.bitWidthForLong(0L));
    assertEquals(1, PforEncoderDecoder.bitWidthForLong(1L));
    assertEquals(2, PforEncoderDecoder.bitWidthForLong(2L));
    assertEquals(2, PforEncoderDecoder.bitWidthForLong(3L));
    assertEquals(3, PforEncoderDecoder.bitWidthForLong(4L));
    assertEquals(8, PforEncoderDecoder.bitWidthForLong(255L));
    assertEquals(9, PforEncoderDecoder.bitWidthForLong(256L));
    assertEquals(16, PforEncoderDecoder.bitWidthForLong(65535L));
    assertEquals(31, PforEncoderDecoder.bitWidthForLong((long) Integer.MAX_VALUE));
    assertEquals(63, PforEncoderDecoder.bitWidthForLong(Long.MAX_VALUE));
    // Unsigned: -1 == 0xFFFFFFFFFFFFFFFF → 64 bits
    assertEquals(64, PforEncoderDecoder.bitWidthForLong(-1L));
  }

  // ========== Cost Model Tests: INT32 ==========

  @Test
  public void testOptimalBitWidthAllIdentical() {
    // All deltas are 0 → bit_width=0, no exceptions
    int[] deltas = new int[1024];
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 1024);
    assertEquals(0, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthNoOutliers() {
    // Deltas 0..255 → all fit in 8 bits, no exceptions
    int[] deltas = new int[256];
    for (int i = 0; i < 256; i++) {
      deltas[i] = i;
    }
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 256);
    assertEquals(8, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthSingleOutlier() {
    // 1023 values fit in 8 bits (0..255), 1 outlier at 100000
    // Cost at bw=8: 1024*8 + 0 = 8192
    // Cost at bw=17: 1024*17 + 0 = 17408
    // Cost at bw=0: 1024*0 + 1024*48 = 49152
    // Single outlier should still pick bw=8: cost=1024*8 + 1*48 = 8240
    int[] deltas = new int[1024];
    for (int i = 0; i < 1023; i++) {
      deltas[i] = i % 256;
    }
    deltas[1023] = 100000;
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 1024);
    assertEquals(8, result.bitWidth);
    assertEquals(1, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthManyOutliers() {
    // All values need 32 bits → bit_width=32, no exceptions
    int[] deltas = new int[100];
    for (int i = 0; i < 100; i++) {
      deltas[i] = Integer.MAX_VALUE - i;
    }
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 100);
    assertEquals(31, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthSingleElement() {
    int[] deltas = {42};
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 1);
    assertEquals(0, result.numExceptions);
    assertTrue(result.bitWidth >= 6); // 42 needs 6 bits
  }

  @Test
  public void testOptimalBitWidthAllZeros() {
    int[] deltas = new int[512];
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 512);
    assertEquals(0, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  // ========== Cost Model Tests: INT64 ==========

  @Test
  public void testOptimalBitWidthLongAllIdentical() {
    long[] deltas = new long[1024];
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltas, 1024);
    assertEquals(0, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthLongNoOutliers() {
    long[] deltas = new long[256];
    for (int i = 0; i < 256; i++) {
      deltas[i] = i;
    }
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltas, 256);
    assertEquals(8, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthLongSingleOutlier() {
    long[] deltas = new long[1024];
    for (int i = 0; i < 1023; i++) {
      deltas[i] = i % 256;
    }
    deltas[1023] = 10_000_000_000L;
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltas, 1024);
    assertEquals(8, result.bitWidth);
    assertEquals(1, result.numExceptions);
  }

  @Test
  public void testOptimalBitWidthLongLargeValues() {
    long[] deltas = new long[100];
    for (int i = 0; i < 100; i++) {
      deltas[i] = Long.MAX_VALUE - i;
    }
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltas, 100);
    assertEquals(63, result.bitWidth);
    assertEquals(0, result.numExceptions);
  }

  // ========== Cost Model Sanity Checks ==========

  @Test
  public void testCostModelPrefersFewerExceptions() {
    // With 50% outliers, the cost model should widen bit width rather than
    // storing half the values as exceptions
    int[] deltas = new int[100];
    for (int i = 0; i < 50; i++) {
      deltas[i] = i;          // 0..49 fit in 6 bits
    }
    for (int i = 50; i < 100; i++) {
      deltas[i] = 1000 + i;   // need ~10 bits
    }
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 100);
    // Should choose to pack everything (10-11 bits) rather than 50 exceptions
    assertEquals(0, result.numExceptions);
  }

  @Test
  public void testCostModelNeverExceedsMaxBitWidth() {
    int[] deltas = {-1}; // 0xFFFFFFFF, needs 32 bits unsigned
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForInt(deltas, 1);
    assertTrue(result.bitWidth <= 32);
  }

  @Test
  public void testCostModelLongNeverExceedsMaxBitWidth() {
    long[] deltas = {-1L}; // 0xFFFFFFFFFFFFFFFF, needs 64 bits unsigned
    PforEncoderDecoder.BitWidthResult result = PforEncoderDecoder.findOptimalBitWidthForLong(deltas, 1);
    assertTrue(result.bitWidth <= 64);
  }
}
