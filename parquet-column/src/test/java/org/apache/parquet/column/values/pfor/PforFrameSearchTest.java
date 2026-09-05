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

import static org.apache.parquet.column.values.pfor.PforConstants.BIT_WIDTH_MASK;
import static org.apache.parquet.column.values.pfor.PforConstants.INT32_VECTOR_INFO_SIZE;
import static org.apache.parquet.column.values.pfor.PforConstants.INT64_VECTOR_INFO_SIZE;
import static org.apache.parquet.column.values.pfor.PforConstants.PFOR_HEADER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.junit.Test;

/**
 * Tests for the searched frame of reference: the frame a vector carries is any lower bound
 * on its values, not necessarily their minimum.
 *
 * <p>What that buys is a packed window sitting where the values cluster, with the values
 * outside it patched. A value below the frame needs no special handling on either side: the
 * writer's subtraction is modular, so it wraps to an offset the packed width cannot hold and
 * fails the same unsigned test as a value above the window, and the exception carries the
 * unreduced value. So the tests here assert both halves -- that the frame does move off the
 * minimum where that is cheaper, and that values on both sides of the window round trip.
 *
 * <p>Nothing on the wire changed, which is the other thing worth pinning: the frame field
 * always held a full-width value and a reader only adds it, so these pages are readable by
 * any reader that could read a minimum-framed one.
 */
public class PforFrameSearchTest {

  private static final long INT32_EXCEPTION_BITS = 16 + 32;
  private static final long INT64_EXCEPTION_BITS = 16 + 64;

  // ---------------------------------------------------------------------------
  // Pages, round trips and wire inspection
  // ---------------------------------------------------------------------------

  private static byte[] intPage(int[] values, int vectorSize) throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      int cap = Math.max(1024, values.length * 8);
      writer = new PforValuesWriter.IntPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize, true);
      for (int v : values) {
        writer.writeInteger(v);
      }
      return toBytes(writer.getBytes());
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private static byte[] longPage(long[] values, int vectorSize) throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      int cap = Math.max(1024, values.length * 16);
      writer = new PforValuesWriter.LongPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize, true);
      for (long v : values) {
        writer.writeLong(v);
      }
      return toBytes(writer.getBytes());
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private static byte[] toBytes(BytesInput bytes) throws Exception {
    ByteBuffer bb = bytes.toByteBuffer();
    byte[] out = new byte[bb.remaining()];
    bb.duplicate().get(out);
    return out;
  }

  private static void assertIntRoundTrip(int[] values, int vectorSize) throws Exception {
    byte[] page = intPage(values, vectorSize);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < values.length; i++) {
      assertEquals("value at " + i, values[i], reader.readInteger());
    }
  }

  private static void assertLongRoundTrip(long[] values, int vectorSize) throws Exception {
    byte[] page = longPage(values, vectorSize);
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < values.length; i++) {
      assertEquals("value at " + i, values[i], reader.readLong());
    }
  }

  private static int intLE(byte[] page, int pos) {
    return (page[pos] & 0xFF)
        | ((page[pos + 1] & 0xFF) << 8)
        | ((page[pos + 2] & 0xFF) << 16)
        | ((page[pos + 3] & 0xFF) << 24);
  }

  private static long longLE(byte[] page, int pos) {
    return (intLE(page, pos) & 0xFFFFFFFFL) | ((long) intLE(page, pos + 4) << 32);
  }

  private static int vectorPos(byte[] page, int vectorIdx) {
    return PFOR_HEADER_SIZE + intLE(page, PFOR_HEADER_SIZE + vectorIdx * Integer.BYTES);
  }

  /** The frame is the first field of a vector's info block. */
  private static int intFrame(byte[] page, int vectorIdx) {
    return intLE(page, vectorPos(page, vectorIdx));
  }

  private static long longFrame(byte[] page, int vectorIdx) {
    return longLE(page, vectorPos(page, vectorIdx));
  }

  private static int intBitWidth(byte[] page, int vectorIdx) {
    return page[vectorPos(page, vectorIdx) + INT32_VECTOR_INFO_SIZE - 3] & BIT_WIDTH_MASK;
  }

  private static int intNumExceptions(byte[] page, int vectorIdx) {
    int pos = vectorPos(page, vectorIdx) + INT32_VECTOR_INFO_SIZE - 2;
    return (page[pos] & 0xFF) | ((page[pos + 1] & 0xFF) << 8);
  }

  private static int longNumExceptions(byte[] page, int vectorIdx) {
    int pos = vectorPos(page, vectorIdx) + INT64_VECTOR_INFO_SIZE - 2;
    return (page[pos] & 0xFF) | ((page[pos + 1] & 0xFF) << 8);
  }

  // ---------------------------------------------------------------------------
  // What the minimum as a frame would have cost, computed independently of the
  // code under test, so the search can be held to never losing against it.
  // ---------------------------------------------------------------------------

  private static long minFrameCostInt(int[] values) {
    int min = values[0];
    for (int v : values) {
      if (v < min) {
        min = v;
      }
    }
    int[] hist = new int[33];
    for (int v : values) {
      hist[PforEncoderDecoder.bitWidthForInt(v - min)]++;
    }
    return bestCost(hist, 32, values.length, INT32_EXCEPTION_BITS);
  }

  private static long minFrameCostLong(long[] values) {
    long min = values[0];
    for (long v : values) {
      if (v < min) {
        min = v;
      }
    }
    int[] hist = new int[65];
    for (long v : values) {
      hist[PforEncoderDecoder.bitWidthForLong(v - min)]++;
    }
    return bestCost(hist, 64, values.length, INT64_EXCEPTION_BITS);
  }

  private static long bestCost(int[] hist, int maxBits, int numElements, long exceptionBits) {
    long best = Long.MAX_VALUE;
    long exceptionsAbove = numElements - hist[0];
    for (int b = 0; b <= maxBits; b++) {
      best = Math.min(best, (long) numElements * b + exceptionsAbove * exceptionBits);
      if (b < maxBits) {
        exceptionsAbove -= hist[b + 1];
      }
    }
    return best;
  }

  // ---------------------------------------------------------------------------
  // Data shapes
  // ---------------------------------------------------------------------------

  /** A tight cluster with one value far below it: the case the minimum cannot handle. */
  private static int[] clusterWithLowOutlier(int count, int base, int spread, int outlier) {
    Random rnd = new Random(7);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = base + rnd.nextInt(spread);
    }
    values[count / 2] = outlier;
    return values;
  }

  private static long[] clusterWithLowOutlierLongs(int count, long base, int spread, long outlier) {
    Random rnd = new Random(7);
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = base + rnd.nextInt(spread);
    }
    values[count / 2] = outlier;
    return values;
  }

  // ---------------------------------------------------------------------------
  // The frame moves off the minimum where that is cheaper
  // ---------------------------------------------------------------------------

  @Test
  public void frameSitsOnTheClusterAndNotOnTheOutlier() {
    int[] values = clusterWithLowOutlier(1024, 1_000_000, 16, 0);
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);

    // The minimum is 0, so a frame there would charge every value the 20 bits that
    // 1,000,015 needs. The frame lands on the cluster instead: four bits of spread, and
    // the one value below the window becomes an exception.
    assertTrue("frame " + plan.frameOfReference + " is above the minimum", plan.frameOfReference >= 1_000_000);
    assertEquals(4, plan.bitWidth);
    assertEquals(1, plan.numExceptions);
    assertTrue(
        "searched cost " + plan.costBits + " beats the minimum's " + minFrameCostInt(values),
        plan.costBits < minFrameCostInt(values));
  }

  @Test
  public void frameSitsOnTheClusterForLongs() {
    long[] values = clusterWithLowOutlierLongs(1024, 1_700_000_000_000L, 16, 0);
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForLong(values, values.length, new long[values.length], false);

    assertTrue("frame " + plan.frameOfReference, plan.frameOfReference >= 1_700_000_000_000L);
    assertEquals(4, plan.bitWidth);
    assertEquals(1, plan.numExceptions);
    assertTrue(plan.costBits < minFrameCostLong(values));
  }

  @Test
  public void oneRepeatedValueAndAnOutlierPacksAtWidthZero() {
    // The window has no width at all: every value but one is the same, so the frame is that
    // value and the outlier is patched. A frame at the minimum would pay 23 bits a value.
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = 5_000_000;
    }
    values[100] = 0;

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertEquals(5_000_000, plan.frameOfReference);
    assertEquals(0, plan.bitWidth);
    assertEquals(1, plan.numExceptions);
    assertEquals(INT32_EXCEPTION_BITS, plan.costBits);
  }

  @Test
  public void aFrameOnTheClusterIsWrittenToThePage() throws Exception {
    int[] values = clusterWithLowOutlier(1024, 1_000_000, 16, 0);
    byte[] page = intPage(values, 1024);

    // The searched frame is what the page carries, in the field that always held it.
    assertTrue("wire frame " + intFrame(page, 0), intFrame(page, 0) >= 1_000_000);
    assertEquals(4, intBitWidth(page, 0));
    assertEquals(1, intNumExceptions(page, 0));
    assertIntRoundTrip(values, 1024);
  }

  @Test
  public void theSearchDeclinesOnAUniformColumn() {
    // Nothing clusters, so no window can pay for the values it leaves out and the frame
    // stays where PFOR has always put it.
    Random rnd = new Random(11);
    int[] values = new int[1024];
    int min = Integer.MAX_VALUE;
    for (int i = 0; i < values.length; i++) {
      values[i] = rnd.nextInt();
      min = Math.min(min, values[i]);
    }

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertEquals(min, plan.frameOfReference);
    assertEquals(minFrameCostInt(values), plan.costBits);
  }

  @Test
  public void aConstantVectorNeedsNoWidthAndNoPatches() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = -42;
    }
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertEquals(-42, plan.frameOfReference);
    assertEquals(0, plan.bitWidth);
    assertEquals(0, plan.numExceptions);
    assertEquals(0, plan.costBits);
    assertIntRoundTrip(values, 1024);
  }

  // ---------------------------------------------------------------------------
  // Patching on both sides of the window
  // ---------------------------------------------------------------------------

  @Test
  public void valuesBelowAndAboveTheWindowAreBothPatched() throws Exception {
    int[] values = clusterWithLowOutlier(1024, 1_000_000, 16, 0);
    values[900] = 1_100_000; // and one above

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertTrue("frame " + plan.frameOfReference, plan.frameOfReference >= 1_000_000);
    assertEquals(4, plan.bitWidth);
    assertEquals("one below the window and one above it", 2, plan.numExceptions);
    assertIntRoundTrip(values, 1024);
  }

  @Test
  public void valuesBelowAndAboveTheWindowAreBothPatchedForLongs() throws Exception {
    long[] values = clusterWithLowOutlierLongs(1024, 1_700_000_000_000L, 16, 0);
    values[900] = 1_800_000_000_000L;

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForLong(values, values.length, new long[values.length], false);
    assertEquals(2, plan.numExceptions);
    assertLongRoundTrip(values, 1024);
  }

  @Test
  public void manyValuesBelowTheFrameRoundTrip() throws Exception {
    // A tenth of the vector sits below the cluster, spread out, so patching has to handle
    // a crowd of below-frame values and not just one.
    Random rnd = new Random(19);
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = (i % 10 == 0) ? rnd.nextInt(1_000_000) : 8_000_000 + rnd.nextInt(8);
    }
    assertIntRoundTrip(values, 1024);
  }

  @Test
  public void theTypeExtremesRoundTripWithASearchedFrame() throws Exception {
    int[] ints = new int[1024];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = (i % 3 == 0) ? Integer.MIN_VALUE : (i % 3 == 1) ? Integer.MAX_VALUE : 0;
    }
    assertIntRoundTrip(ints, 1024);

    long[] longs = new long[1024];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = (i % 3 == 0) ? Long.MIN_VALUE : (i % 3 == 1) ? Long.MAX_VALUE : 0;
    }
    assertLongRoundTrip(longs, 1024);

    // A cluster at the top of the range, which is where the window has no upper edge to
    // test against and the search has to say so rather than compute one.
    int[] atTheTop = new int[1024];
    for (int i = 0; i < atTheTop.length; i++) {
      atTheTop[i] = Integer.MAX_VALUE - (i & 7);
    }
    atTheTop[500] = Integer.MIN_VALUE;
    assertIntRoundTrip(atTheTop, 1024);
  }

  @Test
  public void everyVectorSizeRoundTrips() throws Exception {
    for (int log = 3; log <= 12; log++) {
      int size = 1 << log;
      int[] values = clusterWithLowOutlier(3 * size + 5, 1_000_000, 16, 0);
      assertIntRoundTrip(values, size);
    }
  }

  @Test
  public void eachVectorSearchesItsOwnFrame() throws Exception {
    // Two vectors with unrelated clusters: the frames are per vector, so neither drags the
    // other's window.
    int[] values = new int[2048];
    for (int i = 0; i < 1024; i++) {
      values[i] = 1_000_000 + (i & 15);
    }
    for (int i = 1024; i < 2048; i++) {
      values[i] = -5_000_000 + (i & 15);
    }
    values[10] = 0;
    values[2000] = 0;

    byte[] page = intPage(values, 1024);
    assertTrue(intFrame(page, 0) >= 1_000_000);
    assertTrue(intFrame(page, 1) <= -5_000_000);
    assertIntRoundTrip(values, 1024);
  }

  // ---------------------------------------------------------------------------
  // The search can never lose to the minimum
  // ---------------------------------------------------------------------------

  @Test
  public void theSearchNeverCostsMoreThanTheMinimumWould() {
    for (long seed = 0; seed < 60; seed++) {
      Random rnd = new Random(seed);
      int count = 1 + rnd.nextInt(2000);
      int shape = (int) (seed % 6);
      int[] values = new int[count];
      for (int i = 0; i < count; i++) {
        switch (shape) {
          case 0:
            values[i] = rnd.nextInt();
            break;
          case 1:
            values[i] = 1_000_000 + rnd.nextInt(64);
            break;
          case 2:
            values[i] = rnd.nextInt(4) == 0 ? rnd.nextInt() : 500_000 + rnd.nextInt(8);
            break;
          case 3:
            values[i] = -rnd.nextInt(1 << 20);
            break;
          case 4:
            values[i] = rnd.nextBoolean() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            break;
          default:
            values[i] = 77;
            break;
        }
      }

      PforEncoderDecoder.VectorPlan plan =
          PforEncoderDecoder.chooseVectorPlanForInt(values, count, new int[count], false);
      assertTrue(
          "seed " + seed + ": searched " + plan.costBits + " vs minimum " + minFrameCostInt(values),
          plan.costBits <= minFrameCostInt(values));
    }
  }

  @Test
  public void theSearchNeverCostsMoreThanTheMinimumWouldForLongs() {
    for (long seed = 0; seed < 60; seed++) {
      Random rnd = new Random(seed);
      int count = 1 + rnd.nextInt(2000);
      int shape = (int) (seed % 4);
      long[] values = new long[count];
      for (int i = 0; i < count; i++) {
        switch (shape) {
          case 0:
            values[i] = rnd.nextLong();
            break;
          case 1:
            values[i] = 1_700_000_000_000L + rnd.nextInt(64);
            break;
          case 2:
            values[i] = rnd.nextInt(4) == 0 ? rnd.nextLong() : -900_000_000_000L + rnd.nextInt(8);
            break;
          default:
            values[i] = rnd.nextBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE;
            break;
        }
      }

      PforEncoderDecoder.VectorPlan plan =
          PforEncoderDecoder.chooseVectorPlanForLong(values, count, new long[count], false);
      assertTrue("seed " + seed, plan.costBits <= minFrameCostLong(values));
    }
  }

  @Test
  public void randomShapesRoundTripWhateverTheFrame() throws Exception {
    for (long seed = 0; seed < 40; seed++) {
      Random rnd = new Random(seed);
      int count = 1 + rnd.nextInt(2500);
      int[] ints = new int[count];
      long[] longs = new long[count];
      for (int i = 0; i < count; i++) {
        boolean outlier = rnd.nextInt(20) == 0;
        ints[i] = outlier ? rnd.nextInt() : 3_000_000 + rnd.nextInt(32);
        longs[i] = outlier ? rnd.nextLong() : 3_000_000_000_000L + rnd.nextInt(32);
      }
      assertIntRoundTrip(ints, 1024);
      assertLongRoundTrip(longs, 1024);
    }
  }

  // ---------------------------------------------------------------------------
  // The frame search and the delta mode compose
  // ---------------------------------------------------------------------------

  @Test
  public void differencesGetASearchedFrameToo() throws Exception {
    // A run of equal steps with one jump in it. The differences are one value repeated
    // plus the jump and the leading zero, so their frame is the step and both of those are
    // patched -- which is only reachable because the frame may sit above the minimum.
    int[] values = new int[1024];
    for (int i = 1; i < values.length; i++) {
      values[i] = values[i - 1] + 7;
    }
    for (int i = 500; i < values.length; i++) {
      values[i] += 1000;
    }

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], true);
    assertTrue("delta mode", plan.delta);
    assertEquals(7, plan.frameOfReference);
    assertEquals(0, plan.bitWidth);
    assertEquals("the leading zero and the jump", 2, plan.numExceptions);
    assertIntRoundTrip(values, 1024);
  }

  @Test
  public void theSearchWorksAtBucketGranularityAndSaysSoByDeclining() throws Exception {
    // The buckets divide the vector's range, so a value far above the cluster coarsens
    // them: here the range is 2,000,000,000, which puts the bucket width at 8,388,608 and
    // sweeps the low outlier into the same bucket as the cluster a million above it. No
    // window can then separate them and the search declines, leaving the frame at the
    // minimum -- the answer PFOR has always given, which is the floor this search
    // guarantees rather than a defect. It is worth pinning because the same approximation
    // is what the C++ and Rust encoders make, so a file written by any of them holds the
    // same frame.
    int[] values = clusterWithLowOutlier(1024, 1_000_000, 16, 0);
    values[900] = 2_000_000_000;

    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertEquals(0, plan.frameOfReference);
    assertEquals(minFrameCostInt(values), plan.costBits);
    assertIntRoundTrip(values, 1024);
  }
}
