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
package org.apache.parquet.column.values.rle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.junit.Test;

public class TestRunLengthBitPackingHybridEncoder {

  private RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder() {
    return getRunLengthBitPackingHybridEncoder(3, 5, 10);
  }

  private RunLengthBitPackingHybridEncoder getRunLengthBitPackingHybridEncoder(
      int bitWidth, int initialCapacity, int pageSize) {
    return new RunLengthBitPackingHybridEncoder(
        bitWidth, initialCapacity, pageSize, new DirectByteBufferAllocator());
  }

  @Test
  public void testRLEOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(4);
    }
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 100 << 1 = 200
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(200);
    // payload = 4
    assertThat(BytesUtils.readIntLittleEndianOnOneByte(is)).isEqualTo(4);

    // header = 100 << 1 = 200
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(200);
    // payload = 5
    assertThat(BytesUtils.readIntLittleEndianOnOneByte(is)).isEqualTo(5);

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testRepeatedZeros() throws Exception {
    // previousValue is initialized to 0
    // make sure that repeated 0s at the beginning
    // of the stream don't trip up the repeat count

    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(20);
    // payload = 4
    assertThat(BytesUtils.readIntLittleEndianOnOneByte(is)).isEqualTo(0);

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testBitWidthZero() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(0, 5, 10);
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(20);

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testBitPackingOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((104/8) << 1) | 1 = 27
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(27);

    List<Integer> values = unpack(3, 104, is);

    for (int i = 0; i < 100; i++) {
      assertThat((int) values.get(i)).isEqualTo(i % 3);
    }

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testBitPackingOverflow() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // 504 is the max number of values in a bit packed run
    // that still has a header of 1 byte
    // header = ((504/8) << 1) | 1 = 127
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(127);
    List<Integer> values = unpack(3, 504, is);

    for (int i = 0; i < 504; i++) {
      assertThat((int) values.get(i)).isEqualTo(i % 3);
    }

    // there should now be 496 values in another bit-packed run
    // header = ((496/8) << 1) | 1 = 125
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(125);
    values = unpack(3, 496, is);
    for (int i = 0; i < 496; i++) {
      assertThat((int) values.get(i)).isEqualTo((i + 504) % 3);
    }

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testTransitionFromBitPackingToRle() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder();

    // 5 obviously bit-packed values
    encoder.writeInt(0);
    encoder.writeInt(1);
    encoder.writeInt(0);
    encoder.writeInt(1);
    encoder.writeInt(0);

    // three repeated values, that ought to be bit-packed as well
    encoder.writeInt(2);
    encoder.writeInt(2);
    encoder.writeInt(2);

    // lots more repeated values, that should be rle-encoded
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(2);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((8/8) << 1) | 1 = 3
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(3);

    List<Integer> values = unpack(3, 8, is);
    assertThat(values).containsExactly(0, 1, 0, 1, 0, 2, 2, 2);

    // header = 100 << 1 = 200
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(200);
    // payload = 2
    assertThat(BytesUtils.readIntLittleEndianOnOneByte(is)).isEqualTo(2);

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testPaddingZerosOnUnfinishedBitPackedRuns() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(5, 5, 10);
    for (int i = 0; i < 9; i++) {
      encoder.writeInt(i + 1);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((16/8) << 1) | 1 = 5
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(5);

    List<Integer> values = unpack(5, 16, is);

    assertThat(values).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0);

    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testSwitchingModes() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(9, 100, 1000);

    // rle first
    for (int i = 0; i < 25; i++) {
      encoder.writeInt(17);
    }

    // bit-packing
    for (int i = 0; i < 7; i++) {
      encoder.writeInt(7);
    }

    encoder.writeInt(8);
    encoder.writeInt(9);
    encoder.writeInt(10);

    // bit-packing followed by rle
    for (int i = 0; i < 25; i++) {
      encoder.writeInt(6);
    }

    // followed by a different rle
    for (int i = 0; i < 8; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 25 << 1 = 50
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(50);
    // payload = 17, stored in 2 bytes
    assertThat(BytesUtils.readIntLittleEndianOnTwoBytes(is)).isEqualTo(17);

    // header = ((16/8) << 1) | 1 = 5
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(5);
    List<Integer> values = unpack(9, 16, is);
    int v = 0;
    for (int i = 0; i < 7; i++) {
      assertThat((int) values.get(v)).isEqualTo(7);
      v++;
    }

    assertThat((int) values.get(v++)).isEqualTo(8);
    assertThat((int) values.get(v++)).isEqualTo(9);
    assertThat((int) values.get(v++)).isEqualTo(10);

    for (int i = 0; i < 6; i++) {
      assertThat((int) values.get(v)).isEqualTo(6);
      v++;
    }

    // header = 19 << 1 = 38
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(38);
    // payload = 6, stored in 2 bytes
    assertThat(BytesUtils.readIntLittleEndianOnTwoBytes(is)).isEqualTo(6);

    // header = 8 << 1  = 16
    assertThat(BytesUtils.readUnsignedVarInt(is)).isEqualTo(16);
    // payload = 5, stored in 2 bytes
    assertThat(BytesUtils.readIntLittleEndianOnTwoBytes(is)).isEqualTo(5);

    // end of stream
    assertThat(is.read()).isEqualTo(-1);
  }

  @Test
  public void testGroupBoundary() throws Exception {
    byte[] bytes = new byte[2];
    // Create an RLE byte stream that has 3 values (1 literal group) with
    // bit width 2.
    bytes[0] = (1 << 1) | 1;
    bytes[1] = (1 << 0) | (2 << 2) | (3 << 4);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(2, buffer);
    assertThat(decoder.readInt()).isEqualTo(1);
    assertThat(decoder.readInt()).isEqualTo(2);
    assertThat(decoder.readInt()).isEqualTo(3);
    assertThat(buffer.remaining()).isEqualTo(0);
  }

  // ---- Decoder-specific edge case tests ----

  /**
   * Tests the packedPaddedBuffer edge case: when the last packed run in the
   * stream has fewer bytes available than required (bytesToRead < bytesRequired).
   * This exercises the zero-padded ByteBuffer fallback path in readNext().
   */
  @Test
  public void testDecoderPaddedBufferEdgeCase() throws Exception {
    // Encode 5 distinct values with bitWidth=3 using scalar writeInt.
    // The encoder will produce a bit-packed run with header indicating 1 group (8 values),
    // but only 5 values are meaningful. The stream has exactly 3 bytes (1 group * 3 bitWidth).
    // To create a truncated stream, we manually construct the packed data with fewer bytes
    // than required for the group count declared in the header.
    int bitWidth = 3;
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(bitWidth, 100, 64000);
    // Write 5 values — encoder will pad to 8 (1 group), producing header for 1 group and 3 bytes
    for (int i = 0; i < 5; i++) encoder.writeInt(i);
    byte[] fullEncoded = encoder.toBytes().toByteArray();

    // Decode normally and verify correctness
    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(fullEncoded));
    for (int i = 0; i < 5; i++) {
      assertThat(decoder.readInt()).as("mismatch at index " + i).isEqualTo(i);
    }

    // Now construct a stream where we artificially increase the group count header
    // to declare more groups than bytes available, forcing the padded buffer path.
    // Header for 2 groups (16 values): (2 << 1) | 1 = 5
    // But only provide bytes for 1 group (3 bytes), so bytesToRead < bytesRequired.
    byte[] truncated = new byte[1 + bitWidth]; // 1 byte header + 3 bytes data
    truncated[0] = 5; // header = 2 groups packed
    System.arraycopy(fullEncoded, 1, truncated, 1, bitWidth); // copy the 3 data bytes

    RunLengthBitPackingHybridDecoder paddedDecoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(truncated));
    // First 5 values should decode correctly from the real bytes
    for (int i = 0; i < 5; i++) {
      assertThat(paddedDecoder.readInt())
          .as("padded mismatch at index " + i)
          .isEqualTo(i);
    }
    // Values 5-7 come from the real packed group's padding (encoder pads with 0s)
    for (int i = 5; i < 8; i++) {
      assertThat(paddedDecoder.readInt()).as("padded zero at index " + i).isEqualTo(0);
    }
    // Values 8-15 come from the zero-padded buffer (no real data) — all should be 0
    for (int i = 8; i < 16; i++) {
      assertThat(paddedDecoder.readInt())
          .as("zero-padded value at index " + i)
          .isEqualTo(0);
    }
  }

  /**
   * Tests the unpack32Values fast path by ensuring that a packed run with
   * exactly 4 or more groups (32+ values) is decoded correctly via scalar readInt.
   */
  @Test
  public void testDecoderUnpack32ValuesFastPath() throws Exception {
    // bitWidth=3 with 40 distinct values forces 5 packed groups (40 values).
    // The decoder should use unpack32Values for the first 4 groups, then
    // unpack8Values for the last group.
    int bitWidth = 3;
    int numValues = 40;
    int[] values = new int[numValues];
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(bitWidth, 100, 64000);
    for (int i = 0; i < numValues; i++) {
      values[i] = i % 7; // stay within 3-bit range
      encoder.writeInt(values[i]);
    }
    byte[] encoded = encoder.toBytes().toByteArray();

    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(encoded));
    for (int i = 0; i < numValues; i++) {
      assertThat(decoder.readInt())
          .as("unpack32 fast path mismatch at " + i)
          .isEqualTo(values[i]);
    }
  }

  @Test
  public void testDecoderUnpack32ValuesExact4Groups() throws Exception {
    // Exactly 4 groups (32 values) — uses only the unpack32Values path, no residual
    int bitWidth = 5;
    int numValues = 32;
    int[] values = new int[numValues];
    RunLengthBitPackingHybridEncoder encoder = getRunLengthBitPackingHybridEncoder(bitWidth, 100, 64000);
    for (int i = 0; i < numValues; i++) {
      values[i] = i % 31;
      encoder.writeInt(values[i]);
    }
    byte[] encoded = encoder.toBytes().toByteArray();

    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(encoded));
    for (int i = 0; i < numValues; i++) {
      assertThat(decoder.readInt()).as("exact 4 groups mismatch at " + i).isEqualTo(values[i]);
    }
  }

  private static List<Integer> unpack(int bitWidth, int numValues, ByteArrayInputStream is) throws Exception {

    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int[] unpacked = new int[8];
    byte[] next8Values = new byte[bitWidth];

    List<Integer> values = new ArrayList<>(numValues);

    while (values.size() < numValues) {
      for (int i = 0; i < bitWidth; i++) {
        next8Values[i] = (byte) is.read();
      }

      packer.unpack8Values(next8Values, 0, unpacked, 0);

      for (int v = 0; v < 8; v++) {
        values.add(unpacked[v]);
      }
    }

    return values;
  }
}
