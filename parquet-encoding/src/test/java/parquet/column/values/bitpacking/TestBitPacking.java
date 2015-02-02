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

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import parquet.Log;
import parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import parquet.column.values.bitpacking.BitPacking.BitPackingWriter;

public class TestBitPacking {
  private static final Log LOG = Log.getLog(TestBitPacking.class);

  @Test
  public void testZero() throws IOException {
    int bitLength = 0;
    int[] vals = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    String expected = "";
    validateEncodeDecode(bitLength, vals, expected);
  }

  @Test
  public void testOne_0() throws IOException {
    int[] vals = {0};
    String expected = "00000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_1() throws IOException {
    int[] vals = {1};
    String expected = "10000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_0_0() throws IOException {
    int[] vals = {0, 0};
    String expected = "00000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_1_1() throws IOException {
    int[] vals = {1, 1};
    String expected = "11000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_9_1s() throws IOException {
    int[] vals = {1, 1, 1, 1, 1, 1, 1, 1, 1};
    String expected = "11111111 10000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_9_0s() throws IOException {
    int[] vals = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    String expected = "00000000 00000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_7_0s_1_1() throws IOException {
    int[] vals = {0, 0, 0, 0, 0, 0, 0, 1};
    String expected = "00000001";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne_9_0s_1_1() throws IOException {
    int[] vals = {0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    String expected = "00000000 01000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testOne() throws IOException {
    int[] vals = {0, 1, 0, 0, 1, 1, 1, 0, 0, 1};
    String expected = "01001110 01000000";
    validateEncodeDecode(1, vals, expected);
  }

  @Test
  public void testTwo() throws IOException {
    int[] vals = {0, 1, 2, 3, 3, 3, 2, 1, 1, 0, 0, 0, 1};
    String expected = "00011011 11111001 01000000 01000000";
    validateEncodeDecode(2, vals, expected);
  }

  @Test
  public void testThree() throws IOException {
    int[] vals = {0, 1, 2, 3, 4, 5, 6, 7, 1};
    String expected =
        "00000101 00111001 01110111 " +
        "00100000";
    validateEncodeDecode(3, vals, expected);
  }

  @Test
  public void testFour() throws IOException {
    int[] vals = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1};
    String expected = "00000001 00100011 01000101 01100111 10001001 10101011 11001101 11101111 00010000";
    validateEncodeDecode(4, vals, expected);
  }

  @Test
  public void testFive() throws IOException {
    int[] vals = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 1};
    String expected =
        "00000000 01000100 00110010 00010100 11000111 " +
        "01000010 01010100 10110110 00110101 11001111 " +
        "10000100 01100101 00111010 01010110 11010111 " +
        "11000110 01110101 10111110 01110111 11011111 " +
        "00001000";
    validateEncodeDecode(5, vals, expected);
  }

  @Test
  public void testSix() throws IOException {
    int[] vals = { 0, 28, 34, 35, 63, 1};
    // 000000, 011100, 100010, 100011, 111111, 000001
    String expected =
        "00000001 11001000 10100011 " +
        "11111100 00010000";
    validateEncodeDecode(6, vals, expected);
  }

  @Test
  public void testSeven() throws IOException {
    int[] vals = { 0, 28, 34, 35, 63, 1, 125, 1, 1};
    // 0000000, 0011100, 0100010, 0100011, 0111111, 0000001, 1111101, 0000001, 0000001
    String expected =
        "00000000 01110001 00010010 00110111 11100000 01111110 10000001 " +
        "00000010";
    validateEncodeDecode(7, vals, expected);
  }

  private void validateEncodeDecode(int bitLength, int[] vals, String expected)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BitPackingWriter w = BitPacking.getBitPackingWriter(bitLength, baos);
    for (int i : vals) {
      w.write(i);
    }
    w.finish();
    byte[] bytes = baos.toByteArray();
    LOG.debug("vals ("+bitLength+"): " + toString(vals));
    LOG.debug("bytes: " + toString(bytes));
    Assert.assertEquals(expected, toString(bytes));
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    BitPackingReader r = BitPacking.createBitPackingReader(bitLength, bais, vals.length);
    int[] result = new int[vals.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.read();
    }
    LOG.debug("result: " + toString(result));
    assertArrayEquals(vals, result);
  }

  public static String toString(int[] vals) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i : vals) {
      if (first) {
        first = false;
      } else {
        sb.append(" ");
      }
      sb.append(i);
    }
    return sb.toString();
  }

  public static String toString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (byte b : bytes) {
      if (first) {
        first = false;
      } else {
        sb.append(" ");
      }
      int i = b < 0 ? 256 + b : b;
      String binaryString = Integer.toBinaryString(i);
      for (int j = binaryString.length(); j<8; ++j) {
        sb.append("0");
      }
      sb.append(binaryString);
    }
    return sb.toString();
  }

}
