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
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class TestBytesUtil {

  @Test
  public void testWidth() {
    assertEquals(0, getWidthFromMaxInt(0));
    assertEquals(1, getWidthFromMaxInt(1));
    assertEquals(2, getWidthFromMaxInt(2));
    assertEquals(2, getWidthFromMaxInt(3));
    assertEquals(3, getWidthFromMaxInt(4));
    assertEquals(3, getWidthFromMaxInt(5));
    assertEquals(3, getWidthFromMaxInt(6));
    assertEquals(3, getWidthFromMaxInt(7));
    assertEquals(4, getWidthFromMaxInt(8));
    assertEquals(4, getWidthFromMaxInt(15));
    assertEquals(5, getWidthFromMaxInt(16));
    assertEquals(5, getWidthFromMaxInt(31));
    assertEquals(6, getWidthFromMaxInt(32));
    assertEquals(6, getWidthFromMaxInt(63));
    assertEquals(7, getWidthFromMaxInt(64));
    assertEquals(7, getWidthFromMaxInt(127));
    assertEquals(8, getWidthFromMaxInt(128));
    assertEquals(8, getWidthFromMaxInt(255));
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
      assertEquals("varint round-trip failed for value " + value, value, result);
      assertEquals("ByteBuffer should be fully consumed for value " + value, 0, buf.remaining());
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
    assertEquals("padded read round-trip failed for bitWidth=" + bitWidth + " value=" + value, value, result);
    assertEquals("ByteBuffer should be fully consumed for bitWidth=" + bitWidth, 0, buf.remaining());
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
        assertEquals("sequential varint mismatch for " + expected, expected, actual);
      }
      assertEquals("ByteBuffer should be fully consumed", 0, buf.remaining());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
