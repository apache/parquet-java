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
package parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import parquet.Log;
import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class TestLemireBitPacking {
  private static final Log LOG = Log.getLog(TestLemireBitPacking.class);

  @Test
  public void testPackUnPack() {
    for (Packer packer : Packer.values()) {
      LOG.debug("");
      LOG.debug("testPackUnPack");
      for (int i = 1; i < 32; i++) {
        LOG.debug("Width: " + i);
        int[] values = generateValues(i);
        int[] unpacked = new int[32];
        {
          packUnpack(packer.newIntPacker(i), values, unpacked);
          LOG.debug("int based Output " + packer.name() + ": " + TestBitPacking.toString(unpacked));
          Assert.assertArrayEquals(packer.name() + " width "+i, values, unpacked);
        }
        {
          packUnpack(packer.newBytePacker(i), values, unpacked);
          LOG.debug("byte based Output " + packer.name() + ": " + TestBitPacking.toString(unpacked));
          Assert.assertArrayEquals(packer.name() + " width "+i, values, unpacked);
        }
      }
    }
  }

  private void packUnpack(IntPacker packer, int[] values, int[] unpacked) {
    int[] packed = new int[packer.getBitWidth()];
    packer.pack32Values(values, 0, packed, 0);
    packer.unpack32Values(packed, 0, unpacked, 0);
  }

  private void packUnpack(BytePacker packer, int[] values, int[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    packer.pack32Values(values, 0, packed, 0);
    packer.unpack32Values(packed, 0, unpacked, 0);
  }

  private int[] generateValues(int bitWidth) {
    int[] values = new int[32];
    for (int j = 0; j < values.length; j++) {
      values[j] = (int)(Math.random() * 100000) % (int)Math.pow(2, bitWidth);
    }
    LOG.debug("Input:  " + TestBitPacking.toString(values));
    return values;
  }

  @Test
  public void testPackUnPackAgainstHandWritten() throws IOException {
    LOG.debug("");
    LOG.debug("testPackUnPackAgainstHandWritten");
    for (int i = 1; i < 8; i++) {
      LOG.debug("Width: " + i);
      int[] packed = new int[i];
      int[] unpacked = new int[32];
      int[] values = generateValues(i);

      // pack lemire
      final IntPacker packer = Packer.BIG_ENDIAN.newIntPacker(i);
      packer.pack32Values(values, 0, packed, 0);
      // convert to ints
      final ByteArrayOutputStream lemireOut = new ByteArrayOutputStream();
      for (int v : packed) {
        lemireOut.write((v >>> 24) & 0xFF);
        lemireOut.write((v >>> 16) & 0xFF);
        lemireOut.write((v >>>  8) & 0xFF);
        lemireOut.write((v >>>  0) & 0xFF);
      }
      final byte[] packedByLemireAsBytes = lemireOut.toByteArray();
      LOG.debug("Lemire: " + TestBitPacking.toString(packedByLemireAsBytes));

      // pack manual
      final ByteArrayOutputStream manualOut = new ByteArrayOutputStream();
      final BitPackingWriter writer = BitPacking.getBitPackingWriter(i, manualOut);
      for (int j = 0; j < values.length; j++) {
        writer.write(values[j]);
      }
      final byte[] packedManualAsBytes = manualOut.toByteArray();
      LOG.debug("Manual: " + TestBitPacking.toString(packedManualAsBytes));

      // unpack manual
      final BitPackingReader reader = BitPacking.createBitPackingReader(i, new ByteArrayInputStream(packedByLemireAsBytes), 32);
      for (int j = 0; j < unpacked.length; j++) {
        unpacked[j] = reader.read();
      }

      LOG.debug("Output: " + TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

}
