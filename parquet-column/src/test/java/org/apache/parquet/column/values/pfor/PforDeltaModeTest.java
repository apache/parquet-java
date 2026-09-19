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
import static org.apache.parquet.column.values.pfor.PforConstants.DELTA_FLAG;
import static org.apache.parquet.column.values.pfor.PforConstants.INT32_VECTOR_INFO_SIZE;
import static org.apache.parquet.column.values.pfor.PforConstants.INT64_VECTOR_INFO_SIZE;
import static org.apache.parquet.column.values.pfor.PforConstants.PFOR_HEADER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

/**
 * Tests for the PFOR delta mode: the writer's per-vector choice between packing the values
 * and packing their differences, and the reader's prefix sum over the latter.
 *
 * <p>The mode is visible on the wire in bit 7 of a vector's bit width byte and in the start
 * value that follows the vector info when that bit is set, so the tests here assert on those
 * bytes as well as on round trips.
 */
public class PforDeltaModeTest {

  // ---------------------------------------------------------------------------
  // Page building and reading
  // ---------------------------------------------------------------------------

  private static byte[] intPage(int[] values, int vectorSize, boolean deltaEnabled) throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      int cap = Math.max(1024, values.length * 8);
      writer = new PforValuesWriter.IntPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize, deltaEnabled);
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

  private static byte[] longPage(long[] values, int vectorSize, boolean deltaEnabled) throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      int cap = Math.max(1024, values.length * 16);
      writer = new PforValuesWriter.LongPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize, deltaEnabled);
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

  private static int[] decodeInts(byte[] page, int count) throws Exception {
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    int[] out = new int[count];
    for (int i = 0; i < count; i++) {
      out[i] = reader.readInteger();
    }
    return out;
  }

  private static long[] decodeLongs(byte[] page, int count) throws Exception {
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    long[] out = new long[count];
    for (int i = 0; i < count; i++) {
      out[i] = reader.readLong();
    }
    return out;
  }

  private static void assertIntRoundTrip(int[] values, int vectorSize, boolean deltaEnabled) throws Exception {
    int[] decoded = decodeInts(intPage(values, vectorSize, deltaEnabled), values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals("value at " + i, values[i], decoded[i]);
    }
  }

  private static void assertLongRoundTrip(long[] values, int vectorSize, boolean deltaEnabled) throws Exception {
    long[] decoded = decodeLongs(longPage(values, vectorSize, deltaEnabled), values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals("value at " + i, values[i], decoded[i]);
    }
  }

  // ---------------------------------------------------------------------------
  // Wire inspection. Offsets in the page are relative to the start of the offset
  // array, which begins right after the fixed header.
  // ---------------------------------------------------------------------------

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

  private static int bitWidthByte(byte[] page, int vectorIdx, int vectorInfoSize) {
    return page[vectorPos(page, vectorIdx) + vectorInfoSize - 3] & 0xFF;
  }

  private static boolean intDeltaFlag(byte[] page, int vectorIdx) {
    return (bitWidthByte(page, vectorIdx, INT32_VECTOR_INFO_SIZE) & DELTA_FLAG) != 0;
  }

  private static boolean longDeltaFlag(byte[] page, int vectorIdx) {
    return (bitWidthByte(page, vectorIdx, INT64_VECTOR_INFO_SIZE) & DELTA_FLAG) != 0;
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
  // Data shapes
  // ---------------------------------------------------------------------------

  private static int[] monotoneInts(int count, int base, int step) {
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = base + i * step;
    }
    return values;
  }

  private static long[] monotoneLongs(int count, long base, long step) {
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = base + i * step;
    }
    return values;
  }

  // ---------------------------------------------------------------------------
  // The mode is chosen where it pays, and shows up on the wire
  // ---------------------------------------------------------------------------

  @Test
  public void monotoneIntsAreWrittenAsDifferences() throws Exception {
    int[] values = monotoneInts(1024, 1_000_000, 7);
    byte[] page = intPage(values, 1024, true);

    assertTrue("delta flag", intDeltaFlag(page, 0));
    // Bit 7 is the flag, so the width has to be read out from under it. Every difference
    // here is 7 except the leading 0, and the frame search sits the frame on 7 and patches
    // that one, which leaves nothing to pack.
    assertEquals(0, bitWidthByte(page, 0, INT32_VECTOR_INFO_SIZE) & BIT_WIDTH_MASK);
    assertEquals(1, intNumExceptions(page, 0));
    // The start value sits between the vector info and the packed residuals.
    assertEquals(values[0], intLE(page, vectorPos(page, 0) + INT32_VECTOR_INFO_SIZE));
    assertIntRoundTrip(values, 1024, true);

    byte[] plain = intPage(values, 1024, false);
    assertFalse("delta declined when disabled", intDeltaFlag(plain, 0));
    assertTrue("differences pack smaller: " + page.length + " vs " + plain.length, page.length < plain.length);
  }

  @Test
  public void monotoneLongsAreWrittenAsDifferences() throws Exception {
    long[] values = monotoneLongs(1024, 1_700_000_000_000L, 7);
    byte[] page = longPage(values, 1024, true);

    assertTrue("delta flag", longDeltaFlag(page, 0));
    assertEquals(0, bitWidthByte(page, 0, INT64_VECTOR_INFO_SIZE) & BIT_WIDTH_MASK);
    assertEquals(1, longNumExceptions(page, 0));
    assertEquals(values[0], longLE(page, vectorPos(page, 0) + INT64_VECTOR_INFO_SIZE));
    assertLongRoundTrip(values, 1024, true);

    byte[] plain = longPage(values, 1024, false);
    assertFalse(longDeltaFlag(plain, 0));
    assertTrue("differences pack smaller: " + page.length + " vs " + plain.length, page.length < plain.length);
  }

  @Test
  public void descendingValuesAreWrittenAsDifferences() throws Exception {
    // Every difference is negative, so the frame of reference is negative and the
    // residuals are what the reader adds it back to before summing.
    int[] values = monotoneInts(1024, 5_000_000, -11);
    byte[] page = intPage(values, 1024, true);
    assertTrue(intDeltaFlag(page, 0));
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void nearMonotoneValuesRoundTrip() throws Exception {
    Random rnd = new Random(1234);
    int[] values = new int[2000];
    values[0] = 42;
    for (int i = 1; i < values.length; i++) {
      values[i] = values[i - 1] + rnd.nextInt(40) - 5;
    }
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void modeIsDecidedPerVector() throws Exception {
    // One monotone vector followed by one that is not: the choice has to differ
    // between them, which is why the flag lives in the vector info.
    Random rnd = new Random(99);
    int[] values = new int[128];
    for (int i = 0; i < 64; i++) {
      values[i] = 500_000 + i * 3;
    }
    for (int i = 64; i < 128; i++) {
      values[i] = rnd.nextInt();
    }

    byte[] page = intPage(values, 64, true);
    assertTrue("monotone vector uses the mode", intDeltaFlag(page, 0));
    assertFalse("random vector does not", intDeltaFlag(page, 1));
    assertIntRoundTrip(values, 64, true);
  }

  @Test
  public void randomValuesDeclineTheMode() throws Exception {
    Random rnd = new Random(7);
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = rnd.nextInt();
    }
    byte[] page = intPage(values, 1024, true);
    assertFalse(intDeltaFlag(page, 0));
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void constantVectorStaysPlain() throws Exception {
    // A vector already packing at width 0 cannot be improved on, and differencing it
    // would only add a start value.
    int[] values = new int[1024];
    java.util.Arrays.fill(values, -7);
    byte[] page = intPage(values, 1024, true);
    assertFalse(intDeltaFlag(page, 0));
    assertEquals(0, bitWidthByte(page, 0, INT32_VECTOR_INFO_SIZE) & BIT_WIDTH_MASK);
    assertIntRoundTrip(values, 1024, true);
  }

  // ---------------------------------------------------------------------------
  // Wrapping. Both the differencing and the sum are modular, so values that step
  // across the ends of the range have to come back unchanged.
  // ---------------------------------------------------------------------------

  @Test
  public void intSequenceWrappingPastMaxRoundTrips() throws Exception {
    int[] values = new int[1024];
    int v = Integer.MAX_VALUE - 200;
    for (int i = 0; i < values.length; i++) {
      values[i] = v++;
    }
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void longSequenceWrappingPastMaxRoundTrips() throws Exception {
    long[] values = new long[1024];
    long v = Long.MAX_VALUE - 200;
    for (int i = 0; i < values.length; i++) {
      values[i] = v++;
    }
    assertLongRoundTrip(values, 1024, true);
  }

  @Test
  public void alternatingExtremesRoundTrip() throws Exception {
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = (i % 2 == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void alternatingLongExtremesRoundTrip() throws Exception {
    long[] values = new long[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = (i % 2 == 0) ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
    assertLongRoundTrip(values, 1024, true);
  }

  @Test
  public void differencesAreTakenUnsigned() throws Exception {
    int[] deltas = new int[2];
    PforEncoderDecoder.computeDeltasForInt(new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE}, 2, deltas);
    assertEquals(0, deltas[0]);
    assertEquals(-1, deltas[1]); // 0xFFFFFFFF, the difference taken modulo 2^32

    long[] longDeltas = new long[2];
    PforEncoderDecoder.computeDeltasForLong(new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, 2, longDeltas);
    assertEquals(0L, longDeltas[0]);
    assertEquals(-1L, longDeltas[1]);
  }

  // ---------------------------------------------------------------------------
  // Exceptions in a delta vector hold differences, so the patch has to land before
  // the prefix sum. Patching after it would leave every later value short by the
  // difference between the exception and the placeholder that stood in for it.
  // ---------------------------------------------------------------------------

  @Test
  public void deltaVectorWithExceptionsRoundTrips() throws Exception {
    int[] values = monotoneInts(1024, 100_000, 3);
    for (int i = 500; i < values.length; i++) {
      values[i] += 40_000_000; // one difference far outside the cluster
    }

    byte[] page = intPage(values, 1024, true);
    assertTrue("delta flag", intDeltaFlag(page, 0));
    assertTrue("the jump is stored as an exception", intNumExceptions(page, 0) >= 1);
    assertIntRoundTrip(values, 1024, true);
  }

  @Test
  public void deltaVectorWithExceptionsRoundTripsForLongs() throws Exception {
    long[] values = monotoneLongs(1024, 1_700_000_000_000L, 5);
    for (int i = 700; i < values.length; i++) {
      values[i] += 1L << 45;
    }

    byte[] page = longPage(values, 1024, true);
    assertTrue(longDeltaFlag(page, 0));
    assertTrue("the jump is stored as an exception", longNumExceptions(page, 0) >= 1);
    assertLongRoundTrip(values, 1024, true);
  }

  @Test
  public void severalExceptionsInOneDeltaVectorRoundTrip() throws Exception {
    int[] values = monotoneInts(1024, 0, 4);
    int bump = 0;
    for (int i = 0; i < values.length; i++) {
      if (i == 10 || i == 300 || i == 301 || i == 1023) {
        bump += 30_000_000;
      }
      values[i] += bump;
    }
    byte[] page = intPage(values, 1024, true);
    assertTrue(intDeltaFlag(page, 0));
    assertTrue(intNumExceptions(page, 0) >= 2);
    assertIntRoundTrip(values, 1024, true);
  }

  // ---------------------------------------------------------------------------
  // Vector sizes, partial vectors, and reads that do not start at a boundary
  // ---------------------------------------------------------------------------

  @Test
  public void allVectorSizesRoundTrip() throws Exception {
    for (int log = 3; log <= 12; log++) {
      int vectorSize = 1 << log;
      int[] ints = monotoneInts(1000, 3_000_000, 9);
      long[] longs = monotoneLongs(1000, 9_000_000_000L, 9);
      assertIntRoundTrip(ints, vectorSize, true);
      assertLongRoundTrip(longs, vectorSize, true);
    }
  }

  @Test
  public void partialLastDeltaVectorRoundTrips() throws Exception {
    // 130 values over vectors of 64 leaves a last vector of 2, which still carries
    // its own start value.
    assertIntRoundTrip(monotoneInts(130, 77, 6), 64, true);
    assertLongRoundTrip(monotoneLongs(130, 77L, 6), 64, true);
  }

  @Test
  public void twoValueDeltaVectorRoundTrips() throws Exception {
    assertIntRoundTrip(new int[] {5, 9}, 8, true);
    assertLongRoundTrip(new long[] {5L, 9L}, 8, true);
  }

  @Test
  public void singleValueVectorHasNoDifferenceToTake() throws Exception {
    byte[] page = intPage(new int[] {12345}, 8, true);
    assertFalse(intDeltaFlag(page, 0));
    assertIntRoundTrip(new int[] {12345}, 8, true);
  }

  @Test
  public void skipInsideAndAcrossDeltaVectors() throws Exception {
    int[] values = monotoneInts(300, 1_000, 4);
    byte[] page = intPage(values, 64, true);
    assertTrue(intDeltaFlag(page, 0));

    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    for (int i = 0; i < 10; i++) {
      assertEquals(values[i], reader.readInteger());
    }
    reader.skip(45); // stops inside vector 0, resumes inside the same vector
    for (int i = 55; i < 60; i++) {
      assertEquals(values[i], reader.readInteger());
    }
    reader.skip(128); // crosses two whole vectors without decoding them
    for (int i = 188; i < 300; i++) {
      assertEquals("value at " + i, values[i], reader.readInteger());
    }
  }

  @Test
  public void skipOneAtATimeThroughDeltaVectors() throws Exception {
    long[] values = monotoneLongs(200, 50L, 3);
    byte[] page = longPage(values, 32, true);
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(values.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < values.length; i++) {
      if (i % 2 == 0) {
        reader.skip();
      } else {
        assertEquals(values[i], reader.readLong());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // The decision itself
  // ---------------------------------------------------------------------------

  @Test
  public void planTakesDifferencesForMonotoneInts() throws Exception {
    int[] values = monotoneInts(1024, 1_000_000, 7);
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], true);

    assertTrue(plan.delta);
    assertEquals(values[0], plan.startValue);
    // The differences are 0 then 7 repeated. A frame of 7 packs the run at width 0 and
    // leaves the leading 0 as the one exception, which costs less than charging every
    // value the 3 bits that a frame at the minimum would need.
    assertEquals(7, plan.frameOfReference);
    assertEquals(0, plan.bitWidth);
    assertEquals(1, plan.numExceptions);
  }

  @Test
  public void planTakesDifferencesForMonotoneLongs() throws Exception {
    long[] values = monotoneLongs(1024, 1_700_000_000_000L, 7);
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForLong(values, values.length, new long[values.length], true);

    assertTrue(plan.delta);
    assertEquals(values[0], plan.startValue);
    assertEquals(7, plan.frameOfReference);
    assertEquals(0, plan.bitWidth);
    assertEquals(1, plan.numExceptions);
  }

  @Test
  public void planKeepsValuesWhenTheModeIsDisabled() throws Exception {
    int[] values = monotoneInts(1024, 1_000_000, 7);
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertFalse(plan.delta);
    assertEquals(0L, plan.startValue);
  }

  @Test
  public void planPaysForTheStartValue() throws Exception {
    // Eight values stepping by 100. Differencing packs them at width 0 with the leading
    // difference patched, 48 bits in all, and the start value costs 32 more -- exactly
    // what the values cost as they stand, so the tie keeps the values.
    int[] shortRun = {0, 100, 200, 300, 400, 500, 600, 700};
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(shortRun, shortRun.length, new int[shortRun.length], true);
    assertFalse("start value is not paid for over 8 values", plan.delta);

    // The same step over a whole vector saves far more than it costs.
    int[] longRun = monotoneInts(1024, 0, 100);
    PforEncoderDecoder.VectorPlan longPlan =
        PforEncoderDecoder.chooseVectorPlanForInt(longRun, longRun.length, new int[longRun.length], true);
    assertTrue(longPlan.delta);
  }

  @Test
  public void planDeclinesDifferencesForRandomValues() throws Exception {
    Random rnd = new Random(31);
    int[] values = new int[1024];
    for (int i = 0; i < values.length; i++) {
      values[i] = rnd.nextInt();
    }
    PforEncoderDecoder.VectorPlan plan =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], true);
    assertFalse(plan.delta);
  }

  @Test
  public void planReportsTheCheaperCost() throws Exception {
    int[] values = monotoneInts(1024, 1_000_000, 7);
    PforEncoderDecoder.VectorPlan delta =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], true);
    PforEncoderDecoder.VectorPlan plain =
        PforEncoderDecoder.chooseVectorPlanForInt(values, values.length, new int[values.length], false);
    assertTrue("delta cost " + delta.costBits + " vs plain " + plain.costBits, delta.costBits < plain.costBits);
    // The reported cost carries the start value, so it is what the two modes were
    // compared on rather than the width alone: nothing packed, one exception at 16 bits
    // of position and 32 of value, and the 32-bit start value.
    assertEquals((16 + 32) + 32L, delta.costBits);
  }

  // ---------------------------------------------------------------------------
  // Fuzz
  // ---------------------------------------------------------------------------

  @Test
  public void randomWalksRoundTrip() throws Exception {
    for (long seed = 0; seed < 20; seed++) {
      Random rnd = new Random(seed);
      int count = 1 + rnd.nextInt(3000);
      int vectorSize = 1 << (3 + rnd.nextInt(8));
      int stepRange = 1 << (1 + rnd.nextInt(30));

      int[] ints = new int[count];
      long[] longs = new long[count];
      ints[0] = rnd.nextInt();
      longs[0] = rnd.nextLong();
      for (int i = 1; i < count; i++) {
        ints[i] = ints[i - 1] + rnd.nextInt(stepRange) - stepRange / 2;
        longs[i] = longs[i - 1] + rnd.nextInt(stepRange) - stepRange / 2;
      }

      assertIntRoundTrip(ints, vectorSize, true);
      assertLongRoundTrip(longs, vectorSize, true);
    }
  }

  @Test
  public void enablingTheModeNeverChangesWhatIsRead() throws Exception {
    for (long seed = 100; seed < 110; seed++) {
      Random rnd = new Random(seed);
      int count = 1 + rnd.nextInt(500);
      int[] values = new int[count];
      values[0] = rnd.nextInt(1000);
      for (int i = 1; i < count; i++) {
        values[i] = values[i - 1] + rnd.nextInt(200) - 50;
      }
      int[] withDelta = decodeInts(intPage(values, 128, true), count);
      int[] withoutDelta = decodeInts(intPage(values, 128, false), count);
      for (int i = 0; i < count; i++) {
        assertEquals(values[i], withDelta[i]);
        assertEquals(values[i], withoutDelta[i]);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // The writer-side property, and the path from it to the bytes
  // ---------------------------------------------------------------------------

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m { required int32 c; }");

  private static ColumnDescriptor column() {
    return SCHEMA.getColumns().get(0);
  }

  @Test
  public void theModeIsOnByDefault() {
    // Leaving it on cannot make a page larger, because the writer keeps whichever mode
    // costs fewer bits.
    assertTrue(ParquetProperties.builder().build().isPforDeltaEnabled(column()));
    assertTrue(ParquetProperties.DEFAULT_IS_PFOR_DELTA_ENABLED);
  }

  @Test
  public void theModeCanBeTurnedOffGloballyAndPerColumn() {
    ParquetProperties off =
        ParquetProperties.builder().withPforDeltaEncoding(false).build();
    assertFalse(off.isPforDeltaEnabled(column()));

    ParquetProperties offForOneColumn =
        ParquetProperties.builder().withPforDeltaEncoding("c", false).build();
    assertFalse(offForOneColumn.isPforDeltaEnabled(column()));
  }

  @Test
  public void copyingThePropertiesKeepsTheSetting() {
    ParquetProperties off =
        ParquetProperties.builder().withPforDeltaEncoding(false).build();
    assertFalse(ParquetProperties.copy(off).build().isPforDeltaEnabled(column()));
  }

  @Test
  public void thePropertyReachesTheBytes() throws Exception {
    int[] values = monotoneInts(1024, 1_000_000, 7);

    byte[] withMode = pforPage(values, true);
    assertTrue("property on, flag set", intDeltaFlag(withMode, 0));

    byte[] withoutMode = pforPage(values, false);
    assertFalse("property off, flag clear", intDeltaFlag(withoutMode, 0));

    int[] decoded = decodeInts(withoutMode, values.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], decoded[i]);
    }
  }

  // Goes through the writer factory rather than constructing the PFOR writer directly,
  // so the property is read where a real writer reads it.
  private static byte[] pforPage(int[] values, boolean deltaEnabled) throws Exception {
    ParquetProperties props = ParquetProperties.builder()
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .withDictionaryEncoding(false)
        .withPforEncoding(true)
        .withPforDeltaEncoding(deltaEnabled)
        .withAllocator(new DirectByteBufferAllocator())
        .build();

    ValuesWriter writer = props.newValuesWriter(column());
    try {
      for (int v : values) {
        writer.writeInteger(v);
      }
      assertEquals(org.apache.parquet.column.Encoding.PFOR, writer.getEncoding());
      return toBytes(writer.getBytes());
    } finally {
      writer.reset();
      writer.close();
    }
  }
}
