/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Assert;
import org.junit.Test;

import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class TestByteBitPacking {

  @Test
  public void testPackUnPack() {
    System.out.println();
    System.out.println("testPackUnPack");
    for (int i = 1; i < 32; i++) {
      System.out.println("Width: " + i);
      int[] unpacked = new int[32];
      int[] values = generateValues(i);
      packUnpack(Packer.BIG_ENDIAN.newBytePacker(i), values, unpacked);
      System.out.println("Output: " + TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width "+i, values, unpacked);
    }
  }

  private void packUnpack(BytePacker packer, int[] values, int[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    packer.pack32Values(values, 0, packed, 0);
    System.out.println("packed: " + TestBitPacking.toString(packed));
    packer.unpack32Values(packed, 0, unpacked, 0);
  }

  private int[] generateValues(int bitWidth) {
    int[] values = new int[32];
    for (int j = 0; j < values.length; j++) {
      values[j] = (int)(Math.random() * 100000) % (int)Math.pow(2, bitWidth);
    }
    System.out.println("Input:  " + TestBitPacking.toString(values));
    return values;
  }

  @Test
  public void testPackUnPackAgainstHandWritten() throws IOException {
    System.out.println();
    System.out.println("testPackUnPackAgainstHandWritten");
    for (int i = 1; i < 8; i++) {
      System.out.println("Width: " + i);
      byte[] packed = new byte[i * 4];
      int[] unpacked = new int[32];
      int[] values = generateValues(i);

      // pack generated
      final BytePacker packer = Packer.BIG_ENDIAN.newBytePacker(i);
      packer.pack32Values(values, 0, packed, 0);

      System.out.println("Generated: " + TestBitPacking.toString(packed));

      // pack manual
      final ByteArrayOutputStream manualOut = new ByteArrayOutputStream();
      final BitPackingWriter writer = BitPacking.getBitPackingWriter(i, manualOut);
      for (int j = 0; j < values.length; j++) {
        writer.write(values[j]);
      }
      final byte[] packedManualAsBytes = manualOut.toByteArray();
      System.out.println("Manual: " + TestBitPacking.toString(packedManualAsBytes));

      // unpack manual
      final BitPackingReader reader = BitPacking.createBitPackingReader(i, new ByteArrayInputStream(packed), 32);
      for (int j = 0; j < unpacked.length; j++) {
        unpacked[j] = reader.read();
      }

      System.out.println("Output: " + TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

  @Test
  public void testPackUnPackAgainstLemire() throws IOException {
    for (Packer pack: Packer.values()) {
      System.out.println();
      System.out.println("testPackUnPackAgainstLemire " + pack.name());
      for (int i = 1; i < 32; i++) {
        System.out.println("Width: " + i);
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
        System.out.println("Lemire out: " + TestBitPacking.toString(packedByLemireAsBytes));

        // pack manual
        final BytePacker bytePacker = pack.newBytePacker(i);
        byte[] packedGenerated = new byte[i * 4];
        bytePacker.pack32Values(values, 0, packedGenerated, 0);
        System.out.println("Gener. out: " + TestBitPacking.toString(packedGenerated));
        Assert.assertEquals(pack.name() + " width " + i, TestBitPacking.toString(packedByLemireAsBytes), TestBitPacking.toString(packedGenerated));

        bytePacker.unpack32Values(packedByLemireAsBytes, 0, unpacked, 0);
        System.out.println("Output: " + TestBitPacking.toString(unpacked));

        Assert.assertArrayEquals("width " + i, values, unpacked);
      }
    }
  }
}
