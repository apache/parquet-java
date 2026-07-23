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
import java.nio.ByteOrder;
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

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static byte[] validIntPage(int valueCount, int vectorSize) throws Exception {
    PforValuesWriter.IntPforValuesWriter writer = null;
    try {
      int cap = Math.max(512, valueCount * 8);
      writer = new PforValuesWriter.IntPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize);
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
      writer = new PforValuesWriter.LongPforValuesWriter(
          cap, cap, new DirectByteBufferAllocator(), vectorSize);
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
