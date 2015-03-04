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
package parquet.column.values.boundedint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import parquet.column.values.boundedint.BoundedIntValuesReader;
import parquet.column.values.boundedint.BoundedIntValuesWriter;

public class TestBoundedColumns {
  private final Random r = new Random(42L);

  @Test
  public void testWriterRepeatNoRepeatAndRepeatUnderThreshold() throws IOException {
    int[] ints = {
        1, 1, 1, 1,
        0,
        0,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 16 2s
        1,
        5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 // 24 5s
        };
    String[] result = {"1",b(1,3),b(4),"0",b(0,3),"0",b(0,3),"1",b(2,3),b(16),"0",b(1,3),"1",b(5,3),b(24)};
    compareOutput(7, ints, result);
  }

  @Test
  public void testWriterNoRepeat() throws IOException {
    int bound = 7;
    int[] ints = { 0, 1, 2, 3, 4, 5, 6, 7};
    String[] result = {"0",b(0,3),"0",b(1,3),"0",b(2,3),"0",b(3,3),"0",b(4,3),"0",b(5,3),"0",b(6,3),"0",b(7,3)};
    compareOutput(bound, ints, result);
  }

  private void compareOutput(int bound, int[] ints, String[] result) throws IOException {
    BoundedIntValuesWriter bicw = new BoundedIntValuesWriter(bound, 64*1024, 64*1024);
    for (int i : ints) {
      bicw.writeInteger(i);
    }
    System.out.println(Arrays.toString(ints));
    System.out.println(Arrays.toString(result));
    byte[] byteArray = bicw.getBytes().toByteArray();
    assertEquals(concat(result), toBinaryString(byteArray, 4));
    BoundedIntValuesReader bicr = new BoundedIntValuesReader(bound);
    bicr.initFromPage(1, byteArray, 0);
    String expected = "";
    String got = "";
    for (int i : ints) {
      expected += " " + i;
      got += " " + bicr.readInteger();
    }
    assertEquals(expected, got);
  }

  private String concat(String[] result) {
    String r = "";
    for (String string : result) {
      r = string + r;
    }
    return r;
  }

  private String b(int i) {
    return b(i,8);
  }

  private String b(int i, int size) {
    String binaryString = Integer.toBinaryString(i);
    while (binaryString.length() < size) {
      binaryString = "0" + binaryString;
    }
    return binaryString;
  }

  public static String toBinaryString(byte[] bytes) {
    return toBinaryString(bytes, 0);
  }

  private static String toBinaryString(byte[] bytes, int offset) {
    String result = "";
    for (int i = offset; i < bytes.length; i++) {
      int b = bytes[i] < 0 ? 256 + bytes[i] : bytes[i];
      String binaryString = Integer.toBinaryString(b);
      while (binaryString.length() < 8) {
        binaryString = "0" + binaryString;
      }
      result = binaryString + result;
    }
    return result;
  }

  @Test
  public void testSerDe() throws Exception {
    int[] valuesPerStripe = new int[] { 50, 100, 700, 1, 200 };
    int totalValuesInStream = 0;
    for (int v : valuesPerStripe) {
      totalValuesInStream += v * 2;
    }

    for (int bound = 1; bound < 8; bound++) {
      System.out.println("bound: "+ bound);
      ByteArrayOutputStream tmp = new ByteArrayOutputStream();

      int[] stream = new int[totalValuesInStream];
      BoundedIntValuesWriter bicw = new BoundedIntValuesWriter(bound, 64 * 1024, 64*1024);
      int idx = 0;
      for (int stripeNum = 0; stripeNum < valuesPerStripe.length; stripeNum++) {
        int next = 0;
        for (int i = 0; i < valuesPerStripe[stripeNum]; i++) {
          int temp = r.nextInt(bound + 1);
          while (next == temp) {
            temp = r.nextInt(bound + 1);
          }
          next = temp;
          stream[idx++] = next;
          int ct;
          if (r.nextBoolean()) {
            stream[idx++] = ct = r.nextInt(1000) + 1;
          } else {
            stream[idx++] = ct = 1;
          }
          for (int j = 0; j < ct; j++) {
            bicw.writeInteger(next);
          }
        }
        bicw.getBytes().writeAllTo(tmp);
        bicw.reset();
      }
      tmp.close();

      byte[] input = tmp.toByteArray();

      BoundedIntValuesReader bicr = new BoundedIntValuesReader(bound);
      idx = 0;
      int offset = 0;
      for (int stripeNum = 0; stripeNum < valuesPerStripe.length; stripeNum++) {
        bicr.initFromPage(1, input, offset);
        offset = bicr.getNextOffset();
        for (int i = 0; i < valuesPerStripe[stripeNum]; i++) {
          int number = stream[idx++];
          int ct = stream[idx++];
          assertTrue(number <= bound);
          assertTrue(ct > 0);
          for (int j = 0; j < ct; j++) {
            assertEquals("Failed on bound ["+bound+"], stripe ["+stripeNum+"], iteration ["+i+"], on count ["+ct+"]", number, bicr.readInteger());
          }
        }
      }
    }
  }
}
