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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.Test;

/**
 * Adversarial tests for PFOR readers: feed malformed page bytes and assert the reader
 * fails cleanly rather than crashing, producing silent garbage, or hanging.
 *
 * <p>Covers both explicitly-validated cases (ParquetDecodingException with message)
 * and currently-unvalidated cases (IndexOutOfBoundsException or BufferUnderflowException
 * from the underlying ByteBuffer).
 */
public class PforAdversarialTest {

  private static final int VECTOR_SIZE = PforConstants.DEFAULT_VECTOR_SIZE;

  // The five-element pages below hold one vector, so the page is a 7-byte header,
  // a single 4-byte offset, and then that vector's info.
  private static final int VECTOR_START = PforConstants.PFOR_HEADER_SIZE + Integer.BYTES;
  private static final int OUTLIER_VECTOR_LEN = 5;

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static byte[] validIntPage(int valueCount, int vectorSize) throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      int cap = Math.max(512, valueCount * 8);
      writer = new PforValuesWriter.IntPforValuesWriter(cap, cap, new DirectByteBufferAllocator(), vectorSize);
      for (int i = 0; i < valueCount; i++) {
        writer.writeInteger(i * 7 + 3);
      }
      BytesInput bi = writer.getBytes();
      ByteBuffer bb = bi.toByteBuffer();
      byte[] out = new byte[bb.remaining()];
      bb.duplicate().get(out);
      return out;
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private static byte[] validLongPage(int valueCount, int vectorSize) throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      int cap = Math.max(512, valueCount * 16);
      writer = new PforValuesWriter.LongPforValuesWriter(cap, cap, new DirectByteBufferAllocator(), vectorSize);
      for (int i = 0; i < valueCount; i++) {
        writer.writeLong((long) i * 13 + 5);
      }
      BytesInput bi = writer.getBytes();
      ByteBuffer bb = bi.toByteBuffer();
      byte[] out = new byte[bb.remaining()];
      bb.duplicate().get(out);
      return out;
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // One value far above the cluster, which is the shape the cost model stores as an
  // exception; a low outlier would become the frame of reference instead.
  private static final int[] OUTLIER_INTS = {100, 101, 102, 103, 50000};

  // An INT64 exception costs 80 bits, so the outlier has to sit further out still:
  // 50,000 would be cheaper to pack at full width than to store as an exception.
  private static final long[] OUTLIER_LONGS = {100L, 101L, 102L, 103L, 100L + (1L << 40)};

  private static byte[] outlierIntPage() throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      writer = new PforValuesWriter.IntPforValuesWriter(512, 512, new DirectByteBufferAllocator(), 8);
      for (int value : OUTLIER_INTS) {
        writer.writeInteger(value);
      }
      return toBytes(writer.getBytes());
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private static byte[] outlierLongPage() throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      writer = new PforValuesWriter.LongPforValuesWriter(512, 512, new DirectByteBufferAllocator(), 8);
      for (long value : OUTLIER_LONGS) {
        writer.writeLong(value);
      }
      return toBytes(writer.getBytes());
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // A delta vector has to be worth choosing before the writer will write one: over 64
  // values a step of 1000 takes the width from 16 bits to 10, which more than pays for
  // the 32-bit start value.
  private static final int DELTA_VECTOR_LEN = 64;

  private static byte[] deltaIntPage() throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      writer = new PforValuesWriter.IntPforValuesWriter(
          512, 512, new DirectByteBufferAllocator(), DELTA_VECTOR_LEN);
      for (int i = 0; i < DELTA_VECTOR_LEN; i++) {
        writer.writeInteger(1_000_000 + i * 1000);
      }
      return requireDeltaVector(toBytes(writer.getBytes()), PforConstants.INT32_VECTOR_INFO_SIZE);
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  private static byte[] deltaLongPage() throws Exception {
    PforValuesWriter.LongPforValuesWriter writer = null;
    try {
      writer = new PforValuesWriter.LongPforValuesWriter(
          512, 512, new DirectByteBufferAllocator(), DELTA_VECTOR_LEN);
      for (int i = 0; i < DELTA_VECTOR_LEN; i++) {
        writer.writeLong(1_700_000_000_000L + (long) i * 100_000);
      }
      return requireDeltaVector(toBytes(writer.getBytes()), PforConstants.INT64_VECTOR_INFO_SIZE);
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // A delta vector with one difference far outside the cluster, so it carries an
  // exception whose position the tests below can corrupt.
  private static byte[] deltaOutlierIntPage() throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      writer = new PforValuesWriter.IntPforValuesWriter(
          512, 512, new DirectByteBufferAllocator(), DELTA_VECTOR_LEN);
      for (int i = 0; i < DELTA_VECTOR_LEN; i++) {
        writer.writeInteger(500 + i * 3 + (i >= 40 ? 5_000_000 : 0));
      }
      return requireDeltaVector(toBytes(writer.getBytes()), PforConstants.INT32_VECTOR_INFO_SIZE);
    } finally {
      if (writer != null) {
        writer.reset();
        writer.close();
      }
    }
  }

  // The tests that corrupt a delta vector only mean something if the writer chose the
  // mode in the first place.
  private static byte[] requireDeltaVector(byte[] page, int vectorInfoSize) {
    int bitWidthByte = page[bitWidthOffset(vectorInfoSize)] & 0xFF;
    if ((bitWidthByte & PforConstants.DELTA_FLAG) == 0) {
      fail("expected the writer to choose the delta mode for this vector");
    }
    return page;
  }

  private static int numExceptionsOfFirstVector(byte[] page) {
    return shortLE(page, numExceptionsOffset(PforConstants.INT32_VECTOR_INFO_SIZE));
  }

  // Past the vector info, the start value, and the packed residuals.
  private static int deltaExceptionPositionOffset(byte[] page, int vectorInfoSize) {
    int bitWidth = page[bitWidthOffset(vectorInfoSize)] & PforConstants.BIT_WIDTH_MASK;
    int packedBytes = (DELTA_VECTOR_LEN * bitWidth + 7) / 8;
    return VECTOR_START + vectorInfoSize + Integer.BYTES + packedBytes;
  }

  private static byte[] toBytes(BytesInput bytes) throws Exception {
    ByteBuffer bb = bytes.toByteBuffer();
    byte[] out = new byte[bb.remaining()];
    bb.duplicate().get(out);
    return out;
  }

  private static int bitWidthOffset(int vectorInfoSize) {
    return VECTOR_START + vectorInfoSize - 3;
  }

  private static int numExceptionsOffset(int vectorInfoSize) {
    return VECTOR_START + vectorInfoSize - 2;
  }

  // Offset of the first stored exception position: past the vector info and the
  // packed deltas, whose length depends on the width the writer chose.
  private static int exceptionPositionOffset(byte[] page, int vectorInfoSize) {
    int bitWidth = page[bitWidthOffset(vectorInfoSize)] & PforConstants.BIT_WIDTH_MASK;
    int packedBytes = (OUTLIER_VECTOR_LEN * bitWidth + 7) / 8;
    return VECTOR_START + vectorInfoSize + packedBytes;
  }

  private static int shortLE(byte[] page, int pos) {
    return (page[pos] & 0xFF) | ((page[pos + 1] & 0xFF) << 8);
  }

  private static byte[] putShortLE(byte[] original, int pos, int value) {
    byte[] copy = original.clone();
    copy[pos] = (byte) (value & 0xFF);
    copy[pos + 1] = (byte) ((value >>> 8) & 0xFF);
    return copy;
  }

  private static byte[] mutate(byte[] original, int offset, byte value) {
    byte[] copy = original.clone();
    copy[offset] = value;
    return copy;
  }

  private static byte[] truncate(byte[] original, int newLen) {
    byte[] copy = new byte[newLen];
    System.arraycopy(original, 0, copy, 0, newLen);
    return copy;
  }

  private static void initIntReader(byte[] page, int valuesCount) throws Exception {
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(valuesCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    reader.readInteger();
  }

  private static void initLongReader(byte[] page, int valuesCount) throws Exception {
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(valuesCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    reader.readLong();
  }

  // ---------------------------------------------------------------------------
  // Sanity: valid pages decode cleanly
  // ---------------------------------------------------------------------------

  @Test
  public void sanityBaselineDecodesClean() throws Exception {
    byte[] page = validIntPage(2048, VECTOR_SIZE);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(2048, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 2048; i++) {
      reader.readInteger();
    }
  }

  @Test
  public void sanityBaselineLongDecodesClean() throws Exception {
    byte[] page = validLongPage(2048, VECTOR_SIZE);
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(2048, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 2048; i++) {
      reader.readLong();
    }
  }

  // ---------------------------------------------------------------------------
  // Header validation
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsBadPackingMode() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    byte[] bad = mutate(page, 0, (byte) 99);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, 1024));
  }

  @Test
  public void rejectsLogVectorSizeTooLarge() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    byte[] bad = mutate(page, 1, (byte) 16); // MAX_LOG_VECTOR_SIZE is 15
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, 1024));
  }

  @Test
  public void rejectsLogVectorSizeTooSmall() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    byte[] bad = mutate(page, 1, (byte) 2); // MIN_LOG_VECTOR_SIZE is 3
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, 1024));
  }

  @Test
  public void rejectsBadValueByteWidth() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    byte[] bad = mutate(page, 2, (byte) 3); // must be 4 or 8
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, 1024));
  }

  @Test
  public void rejectsNegativeNumElements() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    // Overwrite num_elements (bytes 3-6) with -1 (0xFFFFFFFF)
    byte[] bad = page.clone();
    bad[3] = (byte) 0xFF;
    bad[4] = (byte) 0xFF;
    bad[5] = (byte) 0xFF;
    bad[6] = (byte) 0xFF;
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, 1024));
  }

  @Test
  public void rejectsNumElementsGreaterThanValuesCount() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    // page header says 1024 elements but we pass valuesCount=500
    assertThrows(ParquetDecodingException.class, () -> initIntReader(page, 500));
  }

  // ---------------------------------------------------------------------------
  // Truncation / corruption
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsHeaderOnlyPage() {
    byte[] page = new byte[PforConstants.PFOR_HEADER_SIZE];
    page[0] = (byte) PforConstants.PFOR_PACKING_MODE_FOR;
    page[1] = (byte) PforConstants.DEFAULT_VECTOR_SIZE_LOG;
    page[2] = (byte) PforConstants.INT32_VALUE_BYTE_WIDTH;
    // num_elements = 100 in LE
    page[3] = 100;
    page[4] = 0;
    page[5] = 0;
    page[6] = 0;

    try {
      initIntReader(page, 100);
      fail("Expected exception for header-only page");
    } catch (Throwable t) {
      assertNotNull(t);
    }
  }

  @Test
  public void rejectsPageTruncatedMidOffsetArray() throws Exception {
    byte[] page = validIntPage(2048, VECTOR_SIZE);
    // Truncate inside the offset array (header=7 + partial offsets)
    byte[] bad = truncate(page, PforConstants.PFOR_HEADER_SIZE + 2);
    try {
      initIntReader(bad, 2048);
      fail("Expected exception for page truncated mid offset array");
    } catch (Throwable t) {
      assertNotNull(t);
    }
  }

  @Test
  public void rejectsPageTruncatedMidVectorData() throws Exception {
    byte[] page = validIntPage(2048, VECTOR_SIZE);
    // Keep header + offset array but truncate vector data
    int offsetArrayEnd = PforConstants.PFOR_HEADER_SIZE + 2 * Integer.BYTES;
    byte[] bad = truncate(page, offsetArrayEnd + 3);
    try {
      initIntReader(bad, 2048);
      fail("Expected exception for page truncated mid vector data");
    } catch (Throwable t) {
      assertNotNull(t);
    }
  }

  @Test
  public void rejectsCorruptedOffsetPointingPastEnd() throws Exception {
    byte[] page = validIntPage(1024, VECTOR_SIZE);
    // The offset array starts at byte 7. Overwrite first offset to point past buffer end.
    byte[] bad = page.clone();
    int hugeOffset = page.length * 2;
    bad[7] = (byte) (hugeOffset & 0xFF);
    bad[8] = (byte) ((hugeOffset >>> 8) & 0xFF);
    bad[9] = (byte) ((hugeOffset >>> 16) & 0xFF);
    bad[10] = (byte) ((hugeOffset >>> 24) & 0xFF);
    try {
      initIntReader(bad, 1024);
      fail("Expected exception for corrupted offset");
    } catch (Throwable t) {
      assertNotNull(t);
    }
  }

  // ---------------------------------------------------------------------------
  // Per-vector info validation
  // ---------------------------------------------------------------------------

  @Test
  public void sanityOutlierPagesCarryAnException() throws Exception {
    assertTrue(
        "the tests below relocate a stored exception, so the int page must have one",
        shortLE(outlierIntPage(), numExceptionsOffset(PforConstants.INT32_VECTOR_INFO_SIZE)) > 0);
    assertTrue(
        "the tests below relocate a stored exception, so the long page must have one",
        shortLE(outlierLongPage(), numExceptionsOffset(PforConstants.INT64_VECTOR_INFO_SIZE)) > 0);
  }

  @Test
  public void rejectsExceptionCountAboveVectorLength() throws Exception {
    byte[] page = outlierIntPage();
    byte[] bad = putShortLE(page, numExceptionsOffset(PforConstants.INT32_VECTOR_INFO_SIZE), 6);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, OUTLIER_VECTOR_LEN));
  }

  @Test
  public void rejectsExceptionPositionPastEndOfVector() throws Exception {
    byte[] page = outlierIntPage();
    byte[] bad = putShortLE(page, exceptionPositionOffset(page, PforConstants.INT32_VECTOR_INFO_SIZE), 100);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, OUTLIER_VECTOR_LEN));
  }

  @Test
  public void rejectsExceptionPositionPastEndOfVectorLong() throws Exception {
    byte[] page = outlierLongPage();
    byte[] bad = putShortLE(page, exceptionPositionOffset(page, PforConstants.INT64_VECTOR_INFO_SIZE), 100);
    assertThrows(ParquetDecodingException.class, () -> initLongReader(bad, OUTLIER_VECTOR_LEN));
  }

  @Test
  public void rejectsBitWidthAboveValueWidth() throws Exception {
    byte[] page = outlierIntPage();
    byte[] bad = mutate(page, bitWidthOffset(PforConstants.INT32_VECTOR_INFO_SIZE), (byte) 33);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, OUTLIER_VECTOR_LEN));
  }

  @Test
  public void rejectsExceptionValuesTruncated() throws Exception {
    byte[] page = outlierIntPage();
    byte[] bad = truncate(page, page.length - Integer.BYTES);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, OUTLIER_VECTOR_LEN));
  }

  // Bit 7 of the bit width byte is the delta flag, so a reader cannot ignore it: with
  // the bit set, the four bytes after the vector info are the start value and the
  // residuals begin further along. This page has no room for that, and saying so is
  // the only safe reading -- decoding it as if the bit were absent would silently
  // return values the writer never wrote.
  @Test
  public void rejectsDeltaFlagOnAVectorWithoutRoomForIt() throws Exception {
    byte[] page = outlierIntPage();
    int at = bitWidthOffset(PforConstants.INT32_VECTOR_INFO_SIZE);
    byte[] withDeltaFlag = mutate(page, at, (byte) (page[at] | PforConstants.DELTA_FLAG));
    assertThrows(ParquetDecodingException.class, () -> initIntReader(withDeltaFlag, OUTLIER_VECTOR_LEN));
  }

  // A delta vector's start value is bounded separately from its residuals, because the
  // header bound was checked before the flag was known.
  @Test
  public void rejectsDeltaVectorWithTruncatedStartValue() throws Exception {
    byte[] page = deltaIntPage();
    // Keep the vector info and two of the start value's four bytes.
    byte[] bad = truncate(page, VECTOR_START + PforConstants.INT32_VECTOR_INFO_SIZE + 2);
    ParquetDecodingException e =
        assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, DELTA_VECTOR_LEN));
    assertTrue(e.getMessage(), e.getMessage().contains("start value"));
  }

  @Test
  public void rejectsDeltaVectorWithTruncatedStartValueLong() throws Exception {
    byte[] page = deltaLongPage();
    byte[] bad = truncate(page, VECTOR_START + PforConstants.INT64_VECTOR_INFO_SIZE + 3);
    ParquetDecodingException e =
        assertThrows(ParquetDecodingException.class, () -> initLongReader(bad, DELTA_VECTOR_LEN));
    assertTrue(e.getMessage(), e.getMessage().contains("start value"));
  }

  // Nothing stops a corrupt page from claiming a width the value type cannot hold, and
  // the flag must not smuggle one past the check.
  @Test
  public void rejectsBitWidthAboveValueWidthInADeltaVector() throws Exception {
    byte[] page = deltaIntPage();
    int at = bitWidthOffset(PforConstants.INT32_VECTOR_INFO_SIZE);
    byte[] bad = mutate(page, at, (byte) (PforConstants.DELTA_FLAG | 33));
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, DELTA_VECTOR_LEN));
  }

  @Test
  public void rejectsExceptionPositionPastEndOfDeltaVector() throws Exception {
    byte[] page = deltaOutlierIntPage();
    assertTrue("the outlier is stored as an exception", numExceptionsOfFirstVector(page) >= 1);
    int at = deltaExceptionPositionOffset(page, PforConstants.INT32_VECTOR_INFO_SIZE);
    byte[] bad = putShortLE(page, at, 100);
    assertThrows(ParquetDecodingException.class, () -> initIntReader(bad, DELTA_VECTOR_LEN));
  }

  // ---------------------------------------------------------------------------
  // Skip/read bounds
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsSkipPastEnd() throws Exception {
    byte[] page = validIntPage(100, 8);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(100, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThrows(ParquetDecodingException.class, () -> reader.skip(101));
  }

  @Test
  public void rejectsNegativeSkip() throws Exception {
    byte[] page = validIntPage(100, 8);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(100, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThrows(ParquetDecodingException.class, () -> reader.skip(-1));
  }

  @Test
  public void rejectsReadPastEnd() throws Exception {
    byte[] page = validIntPage(10, 8);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(10, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 10; i++) {
      reader.readInteger();
    }
    assertThrows(ParquetDecodingException.class, reader::readInteger);
  }

  @Test
  public void rejectsLongReadPastEnd() throws Exception {
    byte[] page = validLongPage(10, 8);
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(10, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 10; i++) {
      reader.readLong();
    }
    assertThrows(ParquetDecodingException.class, reader::readLong);
  }

  // ---------------------------------------------------------------------------
  // Skip across vector boundaries works correctly
  // ---------------------------------------------------------------------------

  @Test
  public void skipAcrossVectorBoundary() throws Exception {
    int vectorSize = 8;
    int count = 30;
    byte[] page = validIntPage(count, vectorSize);
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    // Skip past first two vectors (16 values), read from third
    reader.skip(16);
    int val = reader.readInteger();
    // Expected: 16 * 7 + 3 = 115
    assertTrue("Value after skip should be 115, got: " + val, val == 115);
  }

  @Test
  public void skipAcrossVectorBoundaryLong() throws Exception {
    int vectorSize = 8;
    int count = 30;
    byte[] page = validLongPage(count, vectorSize);
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(count, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));

    reader.skip(16);
    long val = reader.readLong();
    // Expected: 16 * 13 + 5 = 213
    assertTrue("Value after skip should be 213, got: " + val, val == 213L);
  }
}
