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

import org.junit.Assert;
import org.junit.Test;

import parquet.column.primitive.TestBitPacking;
import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class TestLemireBitPacking {

  @Test
  public void testPackUnPack() {
    System.out.println();
    System.out.println("testPackUnPack");
    for (int i = 1; i < 32; i++) {
      System.out.println("Width: " + i);
      int[] values = new int[32];
      int[] packed = new int[i];
      int[] unpacked = new int[32];
      for (int j = 0; j < values.length; j++) {
        values[j] = (int)(Math.random() * 100000) % (int)Math.pow(2, i); // 67 is prime
      }
      System.out.println("Input:  " + TestBitPacking.toString(values));
      final IntPacker packer = LemireBitPackingBE.getPacker(i);
      packer.pack32Values(values, 0, packed, 0);
      packer.unpack32Values(packed, 0, unpacked, 0);
      System.out.println("Output: " + TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width "+i, values, unpacked);
    }
  }

  @Test
  public void testPackUnPackAgainstHandWritten() throws IOException {
    System.out.println();
    System.out.println("testPackUnPackAgainstHandWritten");
    for (int i = 1; i < 8; i++) {
      System.out.println("Width: " + i);
      int[] values = new int[32];
      int[] packed = new int[i];
      int[] unpacked = new int[32];
      for (int j = 0; j < values.length; j++) {
        values[j] = (int)(Math.random() * 100000) % (int)Math.pow(2, i); // 67 is prime
      }
      System.out.println("Input:  " + TestBitPacking.toString(values));

      // pack lemire
      final IntPacker packer = LemireBitPackingBE.getPacker(i);
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
      System.out.println("Lemire: " + TestBitPacking.toString(packedByLemireAsBytes));

      // pack manual
      final ByteArrayOutputStream manualOut = new ByteArrayOutputStream();
      final BitPackingWriter writer = BitPacking.getBitPackingWriter(i, manualOut);
      for (int j = 0; j < values.length; j++) {
        writer.write(values[j]);
      }
      final byte[] packedManualAsBytes = manualOut.toByteArray();
      System.out.println("Manual: " + TestBitPacking.toString(packedManualAsBytes));

      // unpack manual
      final BitPackingReader reader = BitPacking.createBitPackingReader(i, new ByteArrayInputStream(packedByLemireAsBytes), 32);
      for (int j = 0; j < unpacked.length; j++) {
        unpacked[j] = reader.read();
      }

      System.out.println("Output: " + TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

}
