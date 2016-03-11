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
package org.apache.parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestByteBitPacking {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestByteBitPacking.class);

  @Test
  public void testPackUnPack() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("");
      LOGGER.debug("testPackUnPack");
    }
    for (int i = 1; i < 32; i++) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Width: " + i);
      }
      int[] unpacked = new int[32];
      int[] values = generateValues(i);
      packUnpack(Packer.BIG_ENDIAN.newBytePacker(i), values, unpacked);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Output: " + TestBitPacking.toString(unpacked));
      }
      Assert.assertArrayEquals("width "+i, values, unpacked);
    }
  }

  private void packUnpack(BytePacker packer, int[] values, int[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    packer.pack32Values(values, 0, packed, 0);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("packed: " + TestBitPacking.toString(packed));
    }
    packer.unpack32Values(ByteBuffer.wrap(packed), 0, unpacked, 0);
  }

  private int[] generateValues(int bitWidth) {
    int[] values = new int[32];
    for (int j = 0; j < values.length; j++) {
      values[j] = (int)(Math.random() * 100000) % (int)Math.pow(2, bitWidth);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Input:  " + TestBitPacking.toString(values));
    }
    return values;
  }

  @Test
  public void testPackUnPackAgainstHandWritten() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("");
      LOGGER.debug("testPackUnPackAgainstHandWritten");
    }
    for (int i = 1; i < 8; i++) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Width: " + i);
      }
      byte[] packed = new byte[i * 4];
      int[] unpacked = new int[32];
      int[] values = generateValues(i);

      // pack generated
      final BytePacker packer = Packer.BIG_ENDIAN.newBytePacker(i);
      packer.pack32Values(values, 0, packed, 0);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Generated: " + TestBitPacking.toString(packed));
      }

      // pack manual
      final ByteArrayOutputStream manualOut = new ByteArrayOutputStream();
      final BitPackingWriter writer = BitPacking.getBitPackingWriter(i, manualOut);
      for (int j = 0; j < values.length; j++) {
        writer.write(values[j]);
      }
      final byte[] packedManualAsBytes = manualOut.toByteArray();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Manual: " + TestBitPacking.toString(packedManualAsBytes));
      }

      // unpack manual
      final BitPackingReader reader = BitPacking.createBitPackingReader(i, new ByteArrayInputStream(packed), 32);
      for (int j = 0; j < unpacked.length; j++) {
        unpacked[j] = reader.read();
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Output: " + TestBitPacking.toString(unpacked));
      }
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

  @Test
  public void testPackUnPackAgainstLemire() throws IOException {
    for (Packer pack: Packer.values()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("");
        LOGGER.debug("testPackUnPackAgainstLemire " + pack.name());
      }
      for (int i = 1; i < 32; i++) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Width: " + i);
        }
        int[] packed = new int[i];
        int[] unpacked = new int[32];
        int[] values = generateValues(i);

        // pack lemire
        final IntPacker packer = pack.newIntPacker(i);
        packer.pack32Values(values, 0, packed, 0);
        // convert to bytes
        final ByteArrayOutputStream lemireOut = new ByteArrayOutputStream();
        for (int v : packed) {
          switch(pack) {
          case LITTLE_ENDIAN:
            lemireOut.write((v >>>  0) & 0xFF);
            lemireOut.write((v >>>  8) & 0xFF);
            lemireOut.write((v >>> 16) & 0xFF);
            lemireOut.write((v >>> 24) & 0xFF);
            break;
          case BIG_ENDIAN:
            lemireOut.write((v >>> 24) & 0xFF);
            lemireOut.write((v >>> 16) & 0xFF);
            lemireOut.write((v >>>  8) & 0xFF);
            lemireOut.write((v >>>  0) & 0xFF);
            break;
          }
        }
        final byte[] packedByLemireAsBytes = lemireOut.toByteArray();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Lemire out: " + TestBitPacking.toString(packedByLemireAsBytes));
        }

        // pack manual
        final BytePacker bytePacker = pack.newBytePacker(i);
        byte[] packedGenerated = new byte[i * 4];
        bytePacker.pack32Values(values, 0, packedGenerated, 0);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Gener. out: " + TestBitPacking.toString(packedGenerated));
        }
        Assert.assertEquals(pack.name() + " width " + i, TestBitPacking.toString(packedByLemireAsBytes), TestBitPacking.toString(packedGenerated));

        bytePacker.unpack32Values(ByteBuffer.wrap(packedByLemireAsBytes), 0, unpacked, 0);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Output: " + TestBitPacking.toString(unpacked));
        }

        Assert.assertArrayEquals("width " + i, values, unpacked);
      }
    }
  }
}
