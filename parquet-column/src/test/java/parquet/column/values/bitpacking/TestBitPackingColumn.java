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
import static org.junit.Assert.assertEquals;
import static parquet.column.values.bitpacking.Packer.BIG_ENDIAN;

import java.io.IOException;

import org.junit.Test;

import parquet.Log;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesWriter;

public class TestBitPackingColumn {
  private static final Log LOG = Log.getLog(TestBitPackingColumn.class);

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

  private void validateEncodeDecode(int bitLength, int[] vals, String expected) throws IOException {
    for (PACKING_TYPE type : PACKING_TYPE.values()) {
      LOG.debug(type);
      final int bound = (int)Math.pow(2, bitLength) - 1;
      ValuesWriter w = type.getWriter(bound);
      for (int i : vals) {
        w.writeInteger(i);
      }
      byte[] bytes = w.getBytes().toByteArray();
      LOG.debug("vals ("+bitLength+"): " + TestBitPacking.toString(vals));
      LOG.debug("bytes: " + TestBitPacking.toString(bytes));
      assertEquals(type.toString(), expected, TestBitPacking.toString(bytes));
      ValuesReader r = type.getReader(bound);
      r.initFromPage(vals.length, bytes, 0);
      int[] result = new int[vals.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = r.readInteger();
      }
      LOG.debug("result: " + TestBitPacking.toString(result));
      assertArrayEquals(type + " result: " + TestBitPacking.toString(result), vals, result);
    }
  }

  private static enum PACKING_TYPE {
    BYTE_BASED_MANUAL {
      public ValuesReader getReader(final int bound) {
        return new BitPackingValuesReader(bound);
      }
      public ValuesWriter getWriter(final int bound) {
        return new BitPackingValuesWriter(bound, 32*1024, 64*1024);
      }
    }
    ,
    BYTE_BASED_GENERATED {
      public ValuesReader getReader(final int bound) {
        return new ByteBitPackingValuesReader(bound, BIG_ENDIAN);
      }
      public ValuesWriter getWriter(final int bound) {
        return new ByteBitPackingValuesWriter(bound, BIG_ENDIAN);
      }
    }
    ;
    abstract public ValuesReader getReader(final int bound);
    abstract public ValuesWriter getWriter(final int bound);
  }

}
