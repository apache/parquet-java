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
package org.apache.parquet.column.values.symboltable;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.Test;

/**
 * The data page body's framing: what a writer produces byte for byte, and what a reader refuses.
 *
 * <p>The framing is the part of the encoding that two implementations have to agree on exactly, so
 * the write-side assertions here are on bytes rather than on a round trip.
 */
public class SymbolTablePayloadTest {

  private static final int SLAB_SIZE = 64;
  private static final int PAGE_SIZE = 1 << 20;

  private static SymbolTablePayloadWriter writer(OffsetEncoding offsetEncoding) {
    return new SymbolTablePayloadWriter(
        offsetEncoding, SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance());
  }

  private static byte[] write(OffsetEncoding offsetEncoding, byte[]... values) throws IOException {
    try (SymbolTablePayloadWriter writer = writer(offsetEncoding)) {
      for (byte[] value : values) {
        writer.addValue(value, 0, value.length);
      }
      assertEquals(values.length, writer.valueCount());
      return writer.getBytes().toByteArray();
    }
  }

  private static SymbolTablePayload parse(byte[] body, int pageValueCount) throws IOException {
    return SymbolTablePayload.parse(ByteBufferInputStream.wrap(ByteBuffer.wrap(body)), pageValueCount);
  }

  /** Reads back every value, so a bound that is off by one shows up as wrong bytes. */
  private static List<byte[]> valuesOf(SymbolTablePayload payload) {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < payload.valueCount(); i++) {
      byte[] value = new byte[payload.codeLength(i)];
      System.arraycopy(payload.codes(), payload.codeStart(i), value, 0, value.length);
      values.add(value);
    }
    return values;
  }

  @Test
  public void writesThePlainLayoutByteForByte() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2, 3), bytes(4), bytes(5, 6));

    ByteBuffer expected = ByteBuffer.allocate(9 + 12 + 6).order(ByteOrder.LITTLE_ENDIAN);
    expected.put((byte) 0); // PLAIN offsets
    expected.putInt(3); // number of values
    expected.putInt(12); // offset section length
    expected.putInt(3); // end of the first value
    expected.putInt(4);
    expected.putInt(6);
    expected.put(bytes(1, 2, 3, 4, 5, 6));
    assertArrayEquals(expected.array(), body);
  }

  @Test
  public void writesTheDeltaLayoutWithTheSameHeaderShape() throws IOException {
    byte[] body = write(OffsetEncoding.DELTA_BINARY_PACKED, bytes(1, 2, 3), bytes(4), bytes(5, 6));

    ByteBuffer header = ByteBuffer.wrap(body, 0, 9).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(1, header.get() & 0xFF);
    assertEquals(3, header.getInt());
    int offsetSectionSize = header.getInt();
    assertEquals(body.length - 9 - 6, offsetSectionSize);
    assertArrayEquals(bytes(1, 2, 3, 4, 5, 6), java.util.Arrays.copyOfRange(body, body.length - 6, body.length));
  }

  @Test
  public void roundTripsBothOffsetEncodings() throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      byte[] body = write(offsetEncoding, bytes(1, 2, 3), bytes(4), bytes(5, 6));
      SymbolTablePayload payload = parse(body, 3);

      assertEquals(3, payload.valueCount());
      List<byte[]> values = valuesOf(payload);
      assertArrayEquals(bytes(1, 2, 3), values.get(0));
      assertArrayEquals(bytes(4), values.get(1));
      assertArrayEquals(bytes(5, 6), values.get(2));
    }
  }

  /**
   * Values of zero length are ordinary: two offsets that are equal, not a special case.
   */
  @Test
  public void roundTripsValuesWithNoCodes() throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      byte[] body = write(offsetEncoding, bytes(), bytes(7), bytes(), bytes());
      SymbolTablePayload payload = parse(body, 4);

      assertEquals(4, payload.valueCount());
      List<byte[]> values = valuesOf(payload);
      assertEquals(0, values.get(0).length);
      assertArrayEquals(bytes(7), values.get(1));
      assertEquals(0, values.get(2).length);
      assertEquals(0, values.get(3).length);
    }
  }

  @Test
  public void aPageWithNoValuesIsJustAHeader() throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      byte[] body = write(offsetEncoding);
      assertEquals(SymbolTablePayload.HEADER_SIZE, body.length);
      assertEquals(0, parse(body, 0).valueCount());
    }
  }

  /**
   * Why delta offsets are the default rather than a knob: on a page of short values the plain
   * section is a large fraction of the payload, and almost all of it is the same increment repeated.
   */
  @Test
  public void deltaOffsetsCostFarLessThanPlainOnesOnAPageOfShortValues() throws IOException {
    byte[][] values = new byte[2000][];
    for (int i = 0; i < values.length; i++) {
      values[i] = bytes(i & 0xFF, (i >>> 8) & 0xFF, 0x2C, 0x2E);
    }
    int plain = write(OffsetEncoding.PLAIN, values).length;
    int delta = write(OffsetEncoding.DELTA_BINARY_PACKED, values).length;

    int codeBytes = values.length * 4;
    assertEquals(9 + values.length * 4 + codeBytes, plain);
    assertTrue(
        "delta offsets should cost a small fraction of plain ones, but the payloads were " + delta + " and "
            + plain,
        delta - codeBytes < (plain - codeBytes) / 8);
  }

  @Test
  public void rejectsABodyShorterThanTheHeader() {
    ParquetDecodingException thrown = assertThrows(ParquetDecodingException.class, () -> parse(new byte[8], 1));
    assertTrue(thrown.getMessage().contains("shorter than its 9-byte header"));
  }

  @Test
  public void rejectsAnUnknownOffsetEncoding() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1));
    body[0] = 2;
    assertThrows(ParquetDecodingException.class, () -> parse(body, 1));
  }

  @Test
  public void rejectsMoreValuesThanThePageHeaderDeclares() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1), bytes(2));
    assertThrows(ParquetDecodingException.class, () -> parse(body, 1));
  }

  @Test
  public void rejectsAnOffsetSectionLongerThanTheBody() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1));
    putInt(body, 5, 1000);
    assertThrows(ParquetDecodingException.class, () -> parse(body, 1));
  }

  @Test
  public void rejectsAPlainOffsetSectionOfTheWrongLength() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    // Claim one value's worth of offsets while the section holds two.
    putInt(body, 1, 1);
    assertThrows(ParquetDecodingException.class, () -> parse(body, 2));
  }

  @Test
  public void rejectsOffsetsThatGoBackwards() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 9, 3); // the first value ends past where the second one does
    putInt(body, 13, 1);
    assertThrows(ParquetDecodingException.class, () -> parse(body, 2));
  }

  @Test
  public void rejectsOffsetsThatLeaveCodeBytesUnreachable() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 13, 2); // the last value's codes are dropped
    assertThrows(ParquetDecodingException.class, () -> parse(body, 2));
  }

  @Test
  public void rejectsOffsetsThatRunPastTheCodeSection() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 13, 4);
    assertThrows(ParquetDecodingException.class, () -> parse(body, 2));
  }

  @Test
  public void rejectsAnEmptyPageCarryingAnOffsetSection() throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    byte[] header = new byte[9];
    header[0] = (byte) OffsetEncoding.DELTA_BINARY_PACKED.value();
    putInt(header, 1, 0);
    putInt(header, 5, 4);
    body.write(header);
    body.write(new byte[4]);
    assertThrows(ParquetDecodingException.class, () -> parse(body.toByteArray(), 0));
  }

  private static void putInt(byte[] destination, int position, int value) {
    ByteBuffer.wrap(destination).order(ByteOrder.LITTLE_ENDIAN).putInt(position, value);
  }

  private static byte[] bytes(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }
}
