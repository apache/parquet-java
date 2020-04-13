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

package org.apache.parquet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

/**
 * Some utility methods for generating Binary values that length if fixed (e.g. for FIXED_LEN_BYTE_ARRAY or INT96).
 */
public class FixedBinaryTestUtils {
  public static Binary getFixedBinary(int length, BigInteger bigInt) {
    byte[] array = bigInt.toByteArray();
    if (array.length == length) {
      return Binary.fromConstantByteArray(array);
    } else if (array.length < length) {
      byte[] padded = new byte[length];
      int paddingLength = length - array.length;
      if (bigInt.signum() < 0) {
        Arrays.fill(padded, 0, paddingLength, (byte) 0xFF);
      } else {
        Arrays.fill(padded, 0, paddingLength, (byte) 0x00);
      }
      System.arraycopy(array, 0, padded, paddingLength, array.length);
      return Binary.fromConstantByteArray(padded);
    } else {
      throw new IllegalArgumentException(
          "Specified BigInteger (" + bigInt + ") is too long for fixed bytes (" + array.length + '>' + length + ')');
    }
  }

  public static Binary getFixedBinary(PrimitiveType type, BigInteger bigInt) {
    switch (type.getPrimitiveTypeName()) {
    case FIXED_LEN_BYTE_ARRAY:
      return getFixedBinary(type.getTypeLength(), bigInt);
    case INT96:
      return getFixedBinary(12, bigInt);
    case BINARY:
      return Binary.fromConstantByteArray(bigInt.toByteArray());
    default:
      throw new IllegalArgumentException("Type " + type + " cannot be represented by a Binary");
    }
  }

  @Test
  public void testGetFixedBinary() {
    assertArrayEquals(b(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00),
        getFixedBinary(10, BigInteger.valueOf(Integer.MIN_VALUE)).getBytes());
    assertArrayEquals(b(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF),
        getFixedBinary(11, BigInteger.valueOf(-1)).getBytes());
    assertArrayEquals(b(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
        getFixedBinary(12, BigInteger.valueOf(0)).getBytes());
    assertArrayEquals(b(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01),
        getFixedBinary(13, BigInteger.valueOf(1)).getBytes());
    assertArrayEquals(b(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF),
        getFixedBinary(14, BigInteger.valueOf(Integer.MAX_VALUE)).getBytes());
  }

  public void assertCorrectBytes(byte[] expectedBytes, int length, BigInteger bigInt) {
    byte[] actualBytes = getFixedBinary(length, bigInt).getBytes();
    assertArrayEquals(expectedBytes, actualBytes);
    assertEquals(bigInt, new BigInteger(actualBytes));
  }

  private static byte[] b(int... bytes) {
    byte[] arr = new byte[bytes.length];
    for (int i = 0, n = arr.length; i < n; ++i) {
      arr[i] = (byte) bytes[i];
    }
    return arr;
  }
}
