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
package parquet.column.values.rle;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.bitpacking.Packer;

/**
 * @author Alex Levenson
 */
public class TestRunLengthBitPackingHybridEncoder {

  @Test
  public void testRLEOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5, 10);
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(4);
    }
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(4, BytesUtils.readIntLittleEndianOnOneByte(is));

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 5
    assertEquals(5, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testRepeatedZeros() throws Exception {
    // previousValue is initialized to 0
    // make sure that repeated 0s at the beginning
    // of the stream don't trip up the repeat count

    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5, 10);
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertEquals(20, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(0, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitWidthZero() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(0, 5, 10);
    for (int i = 0; i < 10; i++) {
      encoder.writeInt(0);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 10 << 1 = 20
    assertEquals(20, BytesUtils.readUnsignedVarInt(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitPackingOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5, 10);

    for (int i = 0; i < 100; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((104/8) << 1) | 1 = 27
    assertEquals(27, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(3, 104, is);

    for (int i = 0; i < 100; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testBitPackingOverflow() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5, 10);

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt(i % 3);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // 504 is the max number of values in a bit packed run
    // that still has a header of 1 byte
    // header = ((504/8) << 1) | 1 = 127
    assertEquals(127, BytesUtils.readUnsignedVarInt(is));
    List<Integer> values = unpack(3, 504, is);

    for (int i = 0; i < 504; i++) {
      assertEquals(i % 3, (int) values.get(i));
    }

    // there should now be 496 values in another bit-packed run
    // header = ((496/8) << 1) | 1 = 125
    assertEquals(125, BytesUtils.readUnsignedVarInt(is));
    values = unpack(3, 496, is);
    for (int i = 0; i < 496; i++) {
      assertEquals((i + 504) % 3, (int) values.get(i));
    }

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testTransitionFromBitPackingToRle() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5, 10);

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
    assertEquals(3, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(3, 8, is);
    assertEquals(Arrays.asList(0, 1, 0, 1, 0, 2, 2, 2), values);

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 2
    assertEquals(2, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

  @Test
  public void testPaddingZerosOnUnfinishedBitPackedRuns() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(5, 5, 10);
    for (int i = 0; i < 9; i++) {
      encoder.writeInt(i+1);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = ((16/8) << 1) | 1 = 5
    assertEquals(5, BytesUtils.readUnsignedVarInt(is));

    List<Integer> values = unpack(5, 16, is);

    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0), values);

    assertEquals(-1, is.read());
  }

  @Test
  public void testSwitchingModes() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(9, 100, 1000);

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
    assertEquals(50, BytesUtils.readUnsignedVarInt(is));
    // payload = 17, stored in 2 bytes
    assertEquals(17, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // header = ((16/8) << 1) | 1 = 5
    assertEquals(5, BytesUtils.readUnsignedVarInt(is));
    List<Integer> values = unpack(9, 16, is);
    int v = 0;
    for (int i = 0; i < 7; i++) {
      assertEquals(7, (int) values.get(v));
      v++;
    }

    assertEquals(8, (int) values.get(v++));
    assertEquals(9, (int) values.get(v++));
    assertEquals(10, (int) values.get(v++));

    for (int i = 0; i < 6; i++) {
      assertEquals(6, (int) values.get(v));
      v++;
    }

    // header = 19 << 1 = 38
    assertEquals(38, BytesUtils.readUnsignedVarInt(is));
    // payload = 6, stored in 2 bytes
    assertEquals(6, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // header = 8 << 1  = 16
    assertEquals(16, BytesUtils.readUnsignedVarInt(is));
    // payload = 5, stored in 2 bytes
    assertEquals(5, BytesUtils.readIntLittleEndianOnTwoBytes(is));

    // end of stream
    assertEquals(-1, is.read());
  }


  @Test
  public void testGroupBoundary() throws Exception {
	byte[] bytes = new byte[2];
	// Create an RLE byte stream that has 3 values (1 literal group) with
	// bit width 2.
	bytes[0] = (1 << 1 )| 1;
	bytes[1] = (1 << 0) | (2 << 2) | (3 << 4);
    ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(2, stream);
    assertEquals(decoder.readInt(), 1);
    assertEquals(decoder.readInt(), 2);
    assertEquals(decoder.readInt(), 3);
    assertEquals(stream.available(), 0);
  }

  private static List<Integer> unpack(int bitWidth, int numValues, ByteArrayInputStream is)
    throws Exception {

    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int[] unpacked = new int[8];
    byte[] next8Values = new byte[bitWidth];

    List<Integer> values = new ArrayList<Integer>(numValues);

    while(values.size() < numValues) {
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
