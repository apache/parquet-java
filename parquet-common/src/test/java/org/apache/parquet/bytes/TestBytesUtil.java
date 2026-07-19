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
package org.apache.parquet.bytes;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class TestBytesUtil {

  @Test
  public void testWidth() {
    assertThat(getWidthFromMaxInt(0)).isEqualTo(0);
    assertThat(getWidthFromMaxInt(1)).isEqualTo(1);
    assertThat(getWidthFromMaxInt(2)).isEqualTo(2);
    assertThat(getWidthFromMaxInt(3)).isEqualTo(2);
    assertThat(getWidthFromMaxInt(4)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(5)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(6)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(7)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(8)).isEqualTo(4);
    assertThat(getWidthFromMaxInt(15)).isEqualTo(4);
    assertThat(getWidthFromMaxInt(16)).isEqualTo(5);
    assertThat(getWidthFromMaxInt(31)).isEqualTo(5);
    assertThat(getWidthFromMaxInt(32)).isEqualTo(6);
    assertThat(getWidthFromMaxInt(63)).isEqualTo(6);
    assertThat(getWidthFromMaxInt(64)).isEqualTo(7);
    assertThat(getWidthFromMaxInt(127)).isEqualTo(7);
    assertThat(getWidthFromMaxInt(128)).isEqualTo(8);
    assertThat(getWidthFromMaxInt(255)).isEqualTo(8);
  }

  // ---- ByteBuffer overload tests ----

  @Test
  public void testReadUnsignedVarIntByteBuffer() {
    // Single-byte varint: value 0
    assertVarIntRoundTrip(0);
    // Single-byte varint: value 1
    assertVarIntRoundTrip(1);
    // Single-byte: max single-byte value = 127
    assertVarIntRoundTrip(127);
    // Two-byte varint: 128
    assertVarIntRoundTrip(128);
    // Two-byte varint: 300
    assertVarIntRoundTrip(300);
    // Three-byte varint: 16384
    assertVarIntRoundTrip(16384);
    // Larger value
    assertVarIntRoundTrip(100000);
    // Even larger
    assertVarIntRoundTrip(Integer.MAX_VALUE);
  }

  private void assertVarIntRoundTrip(int value) {
    try {
      // Write using the OutputStream API
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BytesUtils.writeUnsignedVarInt(value, baos);
      byte[] bytes = baos.toByteArray();

      // Read back using the ByteBuffer overload
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      int result = BytesUtils.readUnsignedVarInt(buf);
      assertThat(result).as("varint round-trip failed for value " + value).isEqualTo(value);
      assertThat(buf.remaining())
          .as("ByteBuffer should be fully consumed for value " + value)
          .isEqualTo(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReadIntLittleEndianPaddedOnBitWidthByteBuffer() throws IOException {
    // bitWidth 0 -> 0 bytes -> returns 0
    assertPaddedReadRoundTrip(0, 0);

    // bitWidth 1-8 -> 1 byte
    assertPaddedReadRoundTrip(1, 1);
    assertPaddedReadRoundTrip(7, 127);
    assertPaddedReadRoundTrip(8, 255);

    // bitWidth 9-16 -> 2 bytes
    assertPaddedReadRoundTrip(9, 511);
    assertPaddedReadRoundTrip(16, 65535);

    // bitWidth 17-24 -> 3 bytes
    assertPaddedReadRoundTrip(17, 131071);
    assertPaddedReadRoundTrip(24, 0xFFFFFF);

    // bitWidth 25-32 -> 4 bytes
    assertPaddedReadRoundTrip(25, 33554431);
    assertPaddedReadRoundTrip(32, 0x7FFFFFFF);
    assertPaddedReadRoundTrip(32, -1); // all bits set
  }

  private void assertPaddedReadRoundTrip(int bitWidth, int value) throws IOException {
    // Write using the OutputStream API
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BytesUtils.writeIntLittleEndianPaddedOnBitWidth(baos, value, bitWidth);
    byte[] bytes = baos.toByteArray();

    // Read back using the ByteBuffer overload
    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    int result = BytesUtils.readIntLittleEndianPaddedOnBitWidth(buf, bitWidth);
    assertThat(result)
        .as("padded read round-trip failed for bitWidth=" + bitWidth + " value=" + value)
        .isEqualTo(value);
    assertThat(buf.remaining())
        .as("ByteBuffer should be fully consumed for bitWidth=" + bitWidth)
        .isEqualTo(0);
  }

  @Test
  public void testReadUnsignedVarIntByteBufferMultipleSequential() {
    // Write multiple varints sequentially, then read them back from a single ByteBuffer
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      int[] values = {0, 1, 127, 128, 16384, 100000};
      for (int v : values) {
        BytesUtils.writeUnsignedVarInt(v, baos);
      }
      ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
      for (int expected : values) {
        int actual = BytesUtils.readUnsignedVarInt(buf);
        assertThat(actual)
            .as("sequential varint mismatch for " + expected)
            .isEqualTo(expected);
      }
      assertThat(buf.remaining())
          .as("ByteBuffer should be fully consumed")
          .isEqualTo(0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
