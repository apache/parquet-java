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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.junit.jupiter.api.Test;

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
      assertThat(writer.valueCount()).isEqualTo(values.length);
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
    assertThat(body).isEqualTo(expected.array());
  }

  @Test
  public void writesTheDeltaLayoutWithTheSameHeaderShape() throws IOException {
    byte[] body = write(OffsetEncoding.DELTA_BINARY_PACKED, bytes(1, 2, 3), bytes(4), bytes(5, 6));

    ByteBuffer header = ByteBuffer.wrap(body, 0, 9).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(header.get() & 0xFF).isEqualTo(1);
    assertThat(header.getInt()).isEqualTo(3);
    int offsetSectionSize = header.getInt();
    assertThat(offsetSectionSize).isEqualTo(body.length - 9 - 6);
    assertThat(java.util.Arrays.copyOfRange(body, body.length - 6, body.length))
        .isEqualTo(bytes(1, 2, 3, 4, 5, 6));
  }

  @Test
  public void roundTripsBothOffsetEncodings() throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      byte[] body = write(offsetEncoding, bytes(1, 2, 3), bytes(4), bytes(5, 6));
      SymbolTablePayload payload = parse(body, 3);

      assertThat(payload.valueCount()).isEqualTo(3);
      List<byte[]> values = valuesOf(payload);
      assertThat(values.get(0)).isEqualTo(bytes(1, 2, 3));
      assertThat(values.get(1)).isEqualTo(bytes(4));
      assertThat(values.get(2)).isEqualTo(bytes(5, 6));
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

      assertThat(payload.valueCount()).isEqualTo(4);
      List<byte[]> values = valuesOf(payload);
      assertThat(values.get(0)).isEmpty();
      assertThat(values.get(1)).isEqualTo(bytes(7));
      assertThat(values.get(2)).isEmpty();
      assertThat(values.get(3)).isEmpty();
    }
  }

  @Test
  public void aPageWithNoValuesIsJustAHeader() throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      byte[] body = write(offsetEncoding);
      assertThat(body.length).isEqualTo(SymbolTablePayload.HEADER_SIZE);
      assertThat(parse(body, 0).valueCount()).isEqualTo(0);
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
    assertThat(plain).isEqualTo(9 + values.length * 4 + codeBytes);
    assertThat(delta - codeBytes)
        .as("delta offsets should cost a small fraction of plain ones, but the payloads were " + delta + " and "
            + plain)
        .isLessThan((plain - codeBytes) / 8);
  }

  @Test
  public void rejectsABodyShorterThanTheHeader() {
    assertThatThrownBy(() -> parse(new byte[8], 1))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("shorter than its 9-byte header");
  }

  @Test
  public void rejectsAnUnknownOffsetEncoding() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1));
    body[0] = 2;
    assertThatThrownBy(() -> parse(body, 1)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsMoreValuesThanThePageHeaderDeclares() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1), bytes(2));
    assertThatThrownBy(() -> parse(body, 1)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsAnOffsetSectionLongerThanTheBody() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1));
    putInt(body, 5, 1000);
    assertThatThrownBy(() -> parse(body, 1)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsAPlainOffsetSectionOfTheWrongLength() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    // Claim one value's worth of offsets while the section holds two.
    putInt(body, 1, 1);
    assertThatThrownBy(() -> parse(body, 2)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsOffsetsThatGoBackwards() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 9, 3); // the first value ends past where the second one does
    putInt(body, 13, 1);
    assertThatThrownBy(() -> parse(body, 2)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsOffsetsThatLeaveCodeBytesUnreachable() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 13, 2); // the last value's codes are dropped
    assertThatThrownBy(() -> parse(body, 2)).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectsOffsetsThatRunPastTheCodeSection() throws IOException {
    byte[] body = write(OffsetEncoding.PLAIN, bytes(1, 2), bytes(3));
    putInt(body, 13, 4);
    assertThatThrownBy(() -> parse(body, 2)).isInstanceOf(ParquetDecodingException.class);
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
    assertThatThrownBy(() -> parse(body.toByteArray(), 0)).isInstanceOf(ParquetDecodingException.class);
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
