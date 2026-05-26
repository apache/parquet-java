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
package org.apache.parquet.column.values.alp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.Test;

/**
 * Adversarial tests for ALP readers: feed malformed page bytes and assert the reader
 * fails cleanly rather than crashing, producing silent garbage, or hanging.
 *
 * <p>"Fails cleanly" means raising a meaningful exception — preferably
 * {@link ParquetDecodingException}, but at minimum a typed exception (not a JVM-level
 * crash, infinite loop, or wrong answer). The tests cover both:
 * <ul>
 *   <li>Already-validated cases — the reader explicitly rejects these with a
 *       ParquetDecodingException carrying an explanatory message. These tests pin
 *       the validation behavior in place.
 *   <li>Currently-unvalidated cases (truncation, corrupted offsets) — the reader
 *       relies on the underlying ByteBuffer to surface IndexOutOfBoundsException or
 *       BufferUnderflowException. These tests assert that some Throwable is raised
 *       so the failure mode stays "loud" even if the explicit message is missing.
 * </ul>
 */
public class AlpAdversarialTest {

  // ---------------------------------------------------------------------------
  // Helpers: build a known-good encoded page, then mutate copies of it
  // ---------------------------------------------------------------------------

  /** Build a valid ALP-encoded double page with N clean values. */
  private static byte[] validDoublePage(int valueCount, int vectorSize) throws Exception {
    AlpValuesWriter.DoubleAlpValuesWriter writer = null;
    try {
      int cap = Math.max(512, valueCount * 16);
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize);
      // 2-decimal values — the ALP sweet spot, ensures no exceptions
      for (int i = 0; i < valueCount; i++) {
        writer.writeDouble((i % 1000) / 100.0);
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

  /** Build a valid ALP-encoded float page with N clean values. */
  private static byte[] validFloatPage(int valueCount, int vectorSize) throws Exception {
    AlpValuesWriter.FloatAlpValuesWriter writer = null;
    try {
      int cap = Math.max(256, valueCount * 8);
      writer = new AlpValuesWriter.FloatAlpValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize);
      for (int i = 0; i < valueCount; i++) {
        writer.writeFloat((i % 1000) / 100.0f);
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

  /** Sanity baseline: the known-good page actually decodes cleanly. */
  @Test
  public void sanityBaselineDecodesClean() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 32; i++) reader.readDouble();
  }

  // ---------------------------------------------------------------------------
  // Header-level validation (already-validated paths)
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsBadCompressionMode() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[0] = (byte) 0x99; // mode is at byte 0
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("compression"));
  }

  @Test
  public void rejectsBadIntegerEncoding() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[1] = (byte) 0x99; // integer_encoding is at byte 1
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("integer encoding"));
  }

  @Test
  public void rejectsLogVectorSizeTooLarge() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[2] = (byte) 99; // log_vector_size at byte 2
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("vector size"));
  }

  @Test
  public void rejectsLogVectorSizeTooSmall() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[2] = (byte) 2; // below MIN_LOG_VECTOR_SIZE=3
    assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
  }

  @Test
  public void rejectsNegativeNumElements() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_elements is int32 LE at bytes 3..6 — write -1
    page[3] = (byte) 0xFF;
    page[4] = (byte) 0xFF;
    page[5] = (byte) 0xFF;
    page[6] = (byte) 0xFF;
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("element count"));
  }

  @Test
  public void rejectsNumElementsGreaterThanValuesCount() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_elements stays 32; pass valuesCount=10 (smaller than encoded count)
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, () -> {
      new AlpValuesReaderForDouble().initFromPage(10, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    });
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("exceeds"));
  }

  // ---------------------------------------------------------------------------
  // Vector-level validation (already-validated paths, surface lazily on decode)
  // ---------------------------------------------------------------------------

  /** Helper: find the byte position where the first vector's metadata starts. */
  private static int firstVectorOffset(byte[] page) {
    // header (7) + first 4 bytes of offset array = the offset value itself
    int firstVectorOff = ByteBuffer.wrap(page, 7, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    // offsets are measured from the start of the compression body (after the 7B header)
    return 7 + firstVectorOff;
  }

  @Test
  public void rejectsExponentTooHighDouble() throws Exception {
    byte[] page = validDoublePage(32, 16);
    int v0 = firstVectorOffset(page);
    page[v0] = (byte) 99; // exponent byte
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, reader::readDouble);
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("exponent"));
  }

  @Test
  public void rejectsExponentTooHighFloat() throws Exception {
    byte[] page = validFloatPage(32, 16);
    int v0 = firstVectorOffset(page);
    page[v0] = (byte) 99;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, reader::readFloat);
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("exponent"));
  }

  @Test
  public void rejectsFactorGreaterThanExponent() throws Exception {
    byte[] page = validDoublePage(32, 16);
    int v0 = firstVectorOffset(page);
    page[v0] = (byte) 2; // exponent
    page[v0 + 1] = (byte) 5; // factor > exponent
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, reader::readDouble);
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("factor"));
  }

  @Test
  public void rejectsTooManyExceptions() throws Exception {
    byte[] page = validDoublePage(32, 16);
    int v0 = firstVectorOffset(page);
    // num_exceptions at v0+2, uint16 LE — set to 9999, way more than vectorLen=16
    page[v0 + 2] = (byte) (9999 & 0xFF);
    page[v0 + 3] = (byte) ((9999 >>> 8) & 0xFF);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, reader::readDouble);
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("numexceptions"));
  }

  // ---------------------------------------------------------------------------
  // Currently-unvalidated paths: truncation and corrupted offsets
  // These currently fail with low-level Throwables (BufferUnderflowException,
  // IndexOutOfBoundsException). The tests assert any Throwable is raised so we
  // notice if a regression silently swallows the corruption.
  // ---------------------------------------------------------------------------

  /** Page with only the 7-byte header — nothing else. */
  @Test
  public void rejectsHeaderOnlyPage() {
    byte[] tiny = new byte[]{0x00, 0x00, 0x0A, 0x20, 0x00, 0x00, 0x00}; // 32 elements, log_vec=10
    Throwable t = catchAny(() -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(tiny)));
    });
    assertNotNull("header-only page must raise", t);
  }

  @Test
  public void rejectsPageTruncatedMidOffsetArray() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_vectors = ceil(32/16) = 2, so offset array is 8 bytes. Truncate to chop the 2nd offset.
    byte[] truncated = new byte[7 + 4]; // header + first offset only
    System.arraycopy(page, 0, truncated, 0, truncated.length);
    Throwable t = catchAny(() -> {
      new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(truncated)));
    });
    assertNotNull("truncated offset array must raise", t);
  }

  @Test
  public void rejectsPageTruncatedMidVectorData() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // chop the last 20 bytes — guaranteed to land in the middle of the second vector
    byte[] truncated = new byte[page.length - 20];
    System.arraycopy(page, 0, truncated, 0, truncated.length);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    // initFromPage should still succeed (truncation is inside the vectors section,
    // which initFromPage just slices without parsing). The failure surfaces on decode.
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(truncated)));
    Throwable t = catchAny(() -> {
      for (int i = 0; i < 32; i++) reader.readDouble();
    });
    assertNotNull("truncated vector data must raise on read", t);
  }

  @Test
  public void rejectsCorruptedOffsetPointingPastEnd() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // Offset array starts at byte 7. Overwrite the first offset (uint32 LE) with a huge value.
    page[7] = (byte) 0xFF;
    page[8] = (byte) 0xFF;
    page[9] = (byte) 0xFF;
    page[10] = (byte) 0x7F;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    Throwable t = catchAny(() -> reader.readDouble());
    assertNotNull("corrupted offset must raise on decode", t);
  }

  // ---------------------------------------------------------------------------
  // skip() / read() bounds
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsSkipPastEnd() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThrows(ParquetDecodingException.class, () -> reader.skip(33));
  }

  @Test
  public void rejectsNegativeSkip() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThrows(ParquetDecodingException.class, () -> reader.skip(-1));
  }

  @Test
  public void rejectsReadPastEnd() throws Exception {
    byte[] page = validDoublePage(8, 8);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(8, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 8; i++) reader.readDouble();
    ParquetDecodingException ex = assertThrows(ParquetDecodingException.class, reader::readDouble);
    assertTrue(ex.getMessage(), ex.getMessage().toLowerCase().contains("exhausted"));
  }

  // ---------------------------------------------------------------------------
  // Utility
  // ---------------------------------------------------------------------------

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Throwable;
  }

  /** Catch any Throwable (including low-level RuntimeExceptions / Errors). */
  private static Throwable catchAny(ThrowingRunnable r) {
    try {
      r.run();
    } catch (Throwable t) {
      return t;
    }
    fail("Expected a Throwable but none was raised");
    return null; // unreachable
  }
}
