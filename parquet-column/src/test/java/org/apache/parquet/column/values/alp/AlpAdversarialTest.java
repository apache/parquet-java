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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.ParquetDecodingException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

/**
 * Adversarial tests for ALP readers: feed malformed page bytes and assert the reader
 * fails cleanly rather than crashing, producing silent garbage, or hanging.
 *
 * <p>"Fails cleanly" means raising a meaningful exception — preferably
 * {@link ParquetDecodingException}, but at minimum a typed exception (not a JVM-level
 * crash, infinite loop, wrong answer, or an allocation large enough to exhaust the heap).
 * The tests cover both:
 * <ul>
 *   <li>Header and offset validation — the reader explicitly rejects these with a
 *       ParquetDecodingException carrying an explanatory message. These tests pin
 *       the validation behavior in place.
 *   <li>Truncation inside a vector body — the reader relies on the underlying ByteBuffer
 *       to surface IndexOutOfBoundsException or BufferUnderflowException. These tests
 *       assert a clean typed failure so the mode stays "loud" even without a message.
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
      writer = new AlpValuesWriter.DoubleAlpValuesWriter(cap, cap, new DirectByteBufferAllocator(), vectorSize);
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
      writer = new AlpValuesWriter.FloatAlpValuesWriter(cap, cap, new DirectByteBufferAllocator(), vectorSize);
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
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Unsupported ALP compression mode");
  }

  @Test
  public void rejectsBadIntegerEncoding() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[1] = (byte) 0x99; // integer_encoding is at byte 1
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Unsupported ALP integer encoding");
  }

  @Test
  public void rejectsLogVectorSizeTooLarge() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[2] = (byte) 99; // log_vector_size at byte 2
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP log vector size");
  }

  @Test
  public void rejectsLogVectorSizeTooSmall() throws Exception {
    byte[] page = validDoublePage(32, 16);
    page[2] = (byte) 2; // below MIN_LOG_VECTOR_SIZE=3
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP log vector size");
  }

  @Test
  public void rejectsNegativeNumElements() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_elements is int32 LE at bytes 3..6 — write -1
    page[3] = (byte) 0xFF;
    page[4] = (byte) 0xFF;
    page[5] = (byte) 0xFF;
    page[6] = (byte) 0xFF;
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP element count");
  }

  @Test
  public void rejectsNumElementsGreaterThanValuesCount() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_elements stays 32; pass valuesCount=10 (smaller than encoded count)
    assertThatThrownBy(() -> new AlpValuesReaderForDouble()
            .initFromPage(10, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("exceeds page valuesCount");
  }

  // ---------------------------------------------------------------------------
  // Vector-level validation (already-validated paths, surface lazily on decode)
  // ---------------------------------------------------------------------------

  /** Helper: find the byte position where the first vector's metadata starts. */
  private static int firstVectorOffset(byte[] page) {
    // header (7) + first 4 bytes of offset array = the offset value itself
    int firstVectorOff =
        ByteBuffer.wrap(page, 7, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
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
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP double exponent");
  }

  @Test
  public void rejectsExponentTooHighFloat() throws Exception {
    byte[] page = validFloatPage(32, 16);
    int v0 = firstVectorOffset(page);
    page[v0] = (byte) 99;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readFloat)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP float exponent");
  }

  @Test
  public void rejectsFactorGreaterThanExponent() throws Exception {
    byte[] page = validDoublePage(32, 16);
    int v0 = firstVectorOffset(page);
    page[v0] = (byte) 2; // exponent
    page[v0 + 1] = (byte) 5; // factor > exponent
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP double factor");
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
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP numExceptions");
  }

  @Test
  public void rejectsBitWidthTooLargeDouble() throws Exception {
    byte[] page = validDoublePage(32, 16);
    int v0 = firstVectorOffset(page);
    // Layout: ALP_INFO(4) + frameOfReference(8) then bitWidth byte at v0+12. 99 > 64.
    page[v0 + 12] = (byte) 99;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP double bitWidth");
  }

  @Test
  public void rejectsBitWidthTooLargeFloat() throws Exception {
    byte[] page = validFloatPage(32, 16);
    int v0 = firstVectorOffset(page);
    // Layout: ALP_INFO(4) + frameOfReference(4) then bitWidth byte at v0+8. 99 > 32.
    page[v0 + 8] = (byte) 99;
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readFloat)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Invalid ALP float bitWidth");
  }

  /**
   * A bit width that is legal on its own but larger than the vector actually used makes the packed
   * values overrun into the next vector. Decoding another vector's bytes would be a silent wrong
   * answer, so this must be rejected.
   */
  @Test
  public void rejectsPackedValuesRunningPastVectorEnd() throws Exception {
    // 16 vectors, so the inflated packed body stays inside the page and crosses into the vectors
    // that follow; the last-vector case, which would run off the end of the page, is below.
    byte[] page = validDoublePage(256, 16);
    int v0 = firstVectorOffset(page);
    // bitWidth lives after ALP_INFO(4) + frameOfReference(8). 64 is the maximum legal width, so it
    // passes the width check and then claims far more packed bytes than this vector holds.
    page[v0 + 12] = (byte) 64;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(256, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("packed values end at");
  }

  /**
   * The same forged bit width on the last vector would read past the end of the page. It must be
   * rejected as a decoding error before unpacking, not surface as an IndexOutOfBoundsException.
   */
  @Test
  public void rejectsPackedValuesRunningPastEndOfLastVector() throws Exception {
    byte[] page = validDoublePage(20, 8); // 3 vectors, the last holding 4 values
    int lastOffset = ByteBuffer.wrap(page).order(ByteOrder.LITTLE_ENDIAN).getInt(7 + 2 * Integer.BYTES);
    page[7 + lastOffset + 12] = (byte) 64;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(20, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    reader.skip(16);
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("ALP vector 2 packed values end at");
  }

  /**
   * An exception count that is legal against the vector size can still declare an exception block
   * larger than the vector holds. numExceptions == vectorSize passes the AlpInfo check and then
   * needs 16 * (2 + 8) bytes, far more than this vector's span.
   */
  @Test
  public void rejectsExceptionBlockLargerThanVector() throws Exception {
    byte[] page = validDoublePage(256, 16);
    int v0 = firstVectorOffset(page);
    page[v0 + 2] = (byte) 16; // num_exceptions, uint16 LE
    page[v0 + 3] = (byte) 0;
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(256, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("exceptions needing");
  }

  /**
   * An exception position at or past the end of the vector would write outside the decoded values.
   * Exercises the static reader directly, which is the whole point of it taking its inputs
   * explicitly rather than reaching for fields.
   */
  @Test
  public void rejectsExceptionPositionOutOfBounds() {
    ByteBuffer data = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    data.putShort(0, (short) 3); // in range
    data.putShort(2, (short) 16); // == vectorSize, so one past the last valid slot
    int[] positions = new int[4];

    assertThatThrownBy(() -> AlpValuesReader.readExceptionPositions(data, 0, 2, 16, positions))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("ALP exception position 16 out of bounds for vectorSize 16");
  }

  /** Each vector must span at least its fixed headers, the last one included via the end entry. */
  @Test
  public void rejectsVectorShorterThanItsHeaders() {
    // offset array of 2 entries (8 bytes), then vectors at 8 and 30, data ending at 40.
    assertThatThrownBy(() -> AlpValuesReader.validateVectorOffsets(new int[] {8, 30, 40}, 17, 1000))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("ALP vector 1 at offset 30 spans 10 bytes, fewer than the 17");
  }

  /** No vector can be longer than every value packed at full width and also stored as an exception. */
  @Test
  public void rejectsVectorLongerThanAnyVectorCanBe() {
    int max = AlpValuesReader.maxVectorBytes(16, 17, Double.BYTES);
    assertThat(max).isEqualTo(17 + 16 * 8 + 16 * (2 + 8));
    AlpValuesReader.validateVectorOffsets(new int[] {8, 8 + max, 8 + max + 17}, 17, max);
    assertThatThrownBy(() -> AlpValuesReader.validateVectorOffsets(new int[] {8, 9 + max, 9 + max + 17}, 17, max))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("ALP vector 0 at offset 8 spans " + (max + 1) + " bytes, more than the " + max);
  }

  /** The end entry makes every in-range index valid, and anything else is a caller error. */
  @Test
  public void vectorEndPositionRejectsOutOfRangeIndex() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThat(reader.getVectorEndPosition(1)).isEqualTo(page.length - 7 - 2 * Integer.BYTES);
    for (int bad : new int[] {-1, 2, Integer.MAX_VALUE}) {
      assertThatThrownBy(() -> reader.getVectorEndPosition(bad))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("out of range for 2 vectors");
    }
  }

  /** The last vector is partial whenever the value count is not a whole number of vectors. */
  @Test
  public void vectorSizeAccountsForAPartialLastVector() {
    // 3 vectors of 16 holding 40 values: two full, one holding the remaining 8.
    assertThat(AlpValuesReader.getVectorSize(0, 16, 40, 3)).isEqualTo(16);
    assertThat(AlpValuesReader.getVectorSize(1, 16, 40, 3)).isEqualTo(16);
    assertThat(AlpValuesReader.getVectorSize(2, 16, 40, 3)).isEqualTo(8);
    // An exact multiple leaves the last vector full rather than empty.
    assertThat(AlpValuesReader.getVectorSize(1, 16, 32, 2)).isEqualTo(16);
    assertThat(AlpValuesReader.isLast(1, 2)).isTrue();
    assertThat(AlpValuesReader.isLast(0, 2)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Truncation and corrupted offsets
  // Each of these must fail cleanly: a catchable decoding exception, never an OutOfMemoryError
  // from an unbounded allocation and never a silent wrong-value decode. See assertFailsCleanly.
  // ---------------------------------------------------------------------------

  /** Page with only the 7-byte header — nothing else. */
  @Test
  public void rejectsHeaderOnlyPage() {
    byte[] tiny = new byte[] {0x00, 0x00, 0x0A, 0x20, 0x00, 0x00, 0x00}; // 32 elements, log_vec=10
    assertFailsCleanly(() ->
        new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(tiny))));
  }

  @Test
  public void rejectsPageTruncatedMidOffsetArray() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // num_vectors = ceil(32/16) = 2, so offset array is 8 bytes. Truncate to chop the 2nd offset.
    byte[] truncated = new byte[7 + 4]; // header + first offset only
    System.arraycopy(page, 0, truncated, 0, truncated.length);
    assertFailsCleanly(() -> new AlpValuesReaderForDouble()
        .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(truncated))));
  }

  @Test
  public void rejectsPageTruncatedMidVectorData() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // chop the last 20 bytes — guaranteed to land in the middle of the second vector
    byte[] truncated = new byte[page.length - 20];
    System.arraycopy(page, 0, truncated, 0, truncated.length);
    // The offset array still describes a second vector that the truncated body cannot hold, so
    // this is now caught up front when the offsets are validated against the body length.
    assertFailsCleanly(() -> new AlpValuesReaderForDouble()
        .initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(truncated))));
  }

  @Test
  public void rejectsCorruptedOffsetPointingPastEnd() throws Exception {
    byte[] page = validDoublePage(32, 16);
    // Offset array starts at byte 7. Overwrite the first offset (uint32 LE) with a huge value.
    page[7] = (byte) 0xFF;
    page[8] = (byte) 0xFF;
    page[9] = (byte) 0xFF;
    page[10] = (byte) 0x7F;
    // Offsets are validated against the body length in initFromPage, so a bogus offset is
    // rejected before any vector is decoded rather than reading out of bounds later.
    assertFailsCleanly(() ->
        new AlpValuesReaderForDouble().initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page))));
  }

  // ---------------------------------------------------------------------------
  // skip() / read() bounds
  // ---------------------------------------------------------------------------

  @Test
  public void rejectsSkipPastEnd() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(() -> reader.skip(33))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Cannot skip this many elements");
  }

  @Test
  public void rejectsNegativeSkip() throws Exception {
    byte[] page = validDoublePage(32, 16);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(32, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    assertThatThrownBy(() -> reader.skip(-1))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("Cannot skip this many elements");
  }

  @Test
  public void rejectsReadPastEnd() throws Exception {
    byte[] page = validDoublePage(8, 8);
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(8, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
    for (int i = 0; i < 8; i++) reader.readDouble();
    assertThatThrownBy(reader::readDouble)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("ALP double data was already exhausted");
  }

  // ---------------------------------------------------------------------------
  // Utility
  // ---------------------------------------------------------------------------

  /**
   * Requires {@code action} to fail, and requires that failure to be a clean one.
   *
   * <p>The accepted types are deliberately narrow. An {@link OutOfMemoryError} means a corrupt
   * length drove an unbounded allocation, and an {@link AssertionError} means an assertion inside
   * {@code action} failed; accepting any {@link Throwable} here would let either count as a
   * successful rejection.
   *
   * <p>No message is asserted because these failures come from two sources: the reader's own
   * validation, which carries an explanatory message, and the underlying {@link ByteBuffer}, whose
   * {@link IndexOutOfBoundsException} and {@link BufferUnderflowException} have none. What these
   * tests pin down is that the failure is loud and typed, not what it says.
   */
  private static void assertFailsCleanly(ThrowingCallable action) {
    assertThatThrownBy(action)
        .isInstanceOfAny(
            ParquetDecodingException.class,
            IOException.class,
            IndexOutOfBoundsException.class,
            BufferUnderflowException.class,
            IllegalArgumentException.class);
  }
}
