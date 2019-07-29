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
package org.apache.parquet.schema;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveComparator.BOOLEAN_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.DOUBLE_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.FLOAT_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT64_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT64_COMPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
 * This test verifies all the PrimitiveComparator implementations. The logic of all tests is the same: list the
 * elements to be tested in ascending order and then compare every elements to each other (including the element
 * itself) and expect the related value based on the defined order.
 */
public class TestPrimitiveComparator {

  @Test
  public void testBooleanComparator() {
    Boolean[] valuesInAscendingOrder = { null, false, true };

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Boolean vi = valuesInAscendingOrder[i];
        Boolean vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, BOOLEAN_COMPARATOR.compare(vi, vj));
        if (vi != null && vj != null) {
          assertSignumEquals(vi, vj, exp, BOOLEAN_COMPARATOR.compare(vi.booleanValue(), vj.booleanValue()));
        }
      }
    }

    checkThrowingUnsupportedException(BOOLEAN_COMPARATOR, Boolean.TYPE);
  }

  @Test
  public void testSignedInt32Comparator() {
    testInt32Comparator(SIGNED_INT32_COMPARATOR,
        null,
        Integer.MIN_VALUE,
        -12345,
        -1,
        0,
        1,
        12345,
        Integer.MAX_VALUE);
  }

  @Test
  public void testUnsignedInt32Comparator() {
    testInt32Comparator(UNSIGNED_INT32_COMPARATOR,
        null,
        0,                  // 0x00000000
        1,                  // 0x00000001
        12345,              // 0x00003039
        Integer.MAX_VALUE,  // 0x7FFFFFFF
        Integer.MIN_VALUE,  // 0x80000000
        -12345,             // 0xFFFFCFC7
        -1);                // 0xFFFFFFFF
  }

  private void testInt32Comparator(PrimitiveComparator<Integer> comparator, Integer... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Integer vi = valuesInAscendingOrder[i];
        Integer vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, comparator.compare(vi, vj));
        if (vi != null && vj != null) {
          assertSignumEquals(vi, vj, exp, comparator.compare(vi.intValue(), vj.intValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Integer.TYPE);
  }

  @Test
  public void testSignedInt64Comparator() {
    testInt64Comparator(SIGNED_INT64_COMPARATOR,
        null,
        Long.MIN_VALUE,
        -12345678901L,
        -1L,
        0L,
        1L,
        12345678901L,
        Long.MAX_VALUE);
  }

  @Test
  public void testUnsignedInt64Comparator() {
    testInt64Comparator(UNSIGNED_INT64_COMPARATOR,
        null,
        0L,              // 0x0000000000000000
        1L,              // 0x0000000000000001
        12345678901L,    // 0x00000002DFDC1C35
        Long.MAX_VALUE,  // 0x7FFFFFFFFFFFFFFF
        Long.MIN_VALUE,  // 0x8000000000000000
        -12345678901L,   // 0xFFFFFFFD2023E3CB
        -1L);            // 0xFFFFFFFFFFFFFFFF
  }

  private void testInt64Comparator(PrimitiveComparator<Long> comparator, Long... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Long vi = valuesInAscendingOrder[i];
        Long vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, comparator.compare(vi, vj));
        if (vi != null && vj != null) {
          assertSignumEquals(vi, vj, exp, comparator.compare(vi.longValue(), vj.longValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Long.TYPE);
  }

  @Test
  public void testFloatComparator() {
    Float[] valuesInAscendingOrder = {
        null,
        Float.NEGATIVE_INFINITY,
        -Float.MAX_VALUE,
        -1234.5678F,
        -Float.MIN_VALUE,
        0.0F,
        Float.MIN_VALUE,
        1234.5678F,
        Float.MAX_VALUE,
        Float.POSITIVE_INFINITY };

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Float vi = valuesInAscendingOrder[i];
        Float vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, FLOAT_COMPARATOR.compare(vi, vj));
        if (vi != null && vj != null) {
          assertSignumEquals(vi, vj, exp, FLOAT_COMPARATOR.compare(vi.floatValue(), vj.floatValue()));
        }
      }
    }

    checkThrowingUnsupportedException(FLOAT_COMPARATOR, Float.TYPE);
  }

  @Test
  public void testDoubleComparator() {
    Double[] valuesInAscendingOrder = {
        null,
        Double.NEGATIVE_INFINITY,
        -Double.MAX_VALUE,
        -123456.7890123456789,
        -Double.MIN_VALUE,
        0.0,
        Double.MIN_VALUE,
        123456.7890123456789,
        Double.MAX_VALUE,
        Double.POSITIVE_INFINITY };

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Double vi = valuesInAscendingOrder[i];
        Double vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, DOUBLE_COMPARATOR.compare(vi, vj));
        if (vi != null && vj != null) {
          assertSignumEquals(vi, vj, exp, DOUBLE_COMPARATOR.compare(vi.doubleValue(), vj.doubleValue()));
        }
      }
    }

    checkThrowingUnsupportedException(DOUBLE_COMPARATOR, Double.TYPE);
  }

  @Test
  public void testLexicographicalBinaryComparator() {
    testObjectComparator(UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR,
        null,
        Binary.fromConstantByteArray(new byte[0]),                                           // ||
        Binary.fromConstantByteArray(new byte[] { 127, 127, 0, 127 }, 2, 1),                 // |00|
        Binary.fromCharSequence("aaa"),                                                      // |61|61|61|
        Binary.fromString("aaaa"),                                                           // |61|61|61|61|
        Binary.fromReusedByteArray("aaab".getBytes()),                                       // |61|61|61|62|
        Binary.fromReusedByteArray("azzza".getBytes(), 1, 3),                                // |7A|7A|7A|
        Binary.fromReusedByteBuffer(ByteBuffer.wrap("zzzzzz".getBytes())),                   // |7A|7A|7A|7A|7A|7A|
        Binary.fromReusedByteBuffer(ByteBuffer.wrap("aazzzzzzaa".getBytes(), 2, 7)),         // |7A|7A|7A|7A|7A|7A|61|
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { -128, -128, -128 })),     // |80|80|80|
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { -128, -128, -1 }, 1, 2))  // |80|FF|
    );
  }

  @Test
  public void testBinaryAsSignedIntegerComparator() {
    testObjectComparator(BINARY_AS_SIGNED_INTEGER_COMPARATOR,
        null,
        Binary.fromConstantByteArray(new BigInteger("-9999999999999999999999999999999999999999").toByteArray()),
        Binary.fromReusedByteArray(new BigInteger("-9999999999999999999999999999999999999998").toByteArray()),
        Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toByteArray()),
        Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).toByteArray()),
        Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE).toByteArray()),
        Binary.fromReusedByteArray(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, -2 }, 1, 3),
        Binary.fromReusedByteArray(new BigInteger("-1").toByteArray()),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new BigInteger("0").toByteArray())),
        Binary.fromReusedByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, 0, 1 })),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, 0, 2 }), 2, 2),
        Binary.fromConstantByteBuffer(
            ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE).toByteArray())),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).toByteArray())),
        Binary
            .fromConstantByteBuffer(
                ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toByteArray())),
        Binary.fromConstantByteBuffer(
            ByteBuffer.wrap(new BigInteger("999999999999999999999999999999999999999").toByteArray())),
        Binary.fromReusedByteBuffer(
            ByteBuffer.wrap(new BigInteger("9999999999999999999999999999999999999998").toByteArray())),
        Binary.fromConstantByteBuffer(
            ByteBuffer.wrap(new BigInteger("9999999999999999999999999999999999999999").toByteArray())));
  }

  @Test
  public void testBinaryAsSignedIntegerComparatorWithEquals() {
    List<Binary> valuesToCompare = new ArrayList<>();
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, -108 })));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, 0, 0, 0, -108 })));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, 0, -108 })));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, 0, 0, 0, -108 })));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] { 0, -108 })));

    for (Binary v1 : valuesToCompare) {
      for (Binary v2 : valuesToCompare) {
        assertEquals(String.format("Wrong result of comparison %s and %s", v1, v2),
            0, BINARY_AS_SIGNED_INTEGER_COMPARATOR.compare(v1, v2));
      }
    }
  }

  private <T> void testObjectComparator(PrimitiveComparator<T> comparator, T... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        T vi = valuesInAscendingOrder[i];
        T vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, comparator.compare(vi, vj));
      }
    }

    checkThrowingUnsupportedException(comparator, null);
  }

  private <T> void assertSignumEquals(T v1, T v2, int expected, int actual) {
    String sign = expected < 0 ? " < " : expected > 0 ? " > " : " = ";
    assertEquals("expected: " + v1 + sign + v2, signum(expected), signum(actual));
  }

  private int signum(int i) {
    return i < 0 ? -1 : i > 0 ? 1 : 0;
  }

  private void checkThrowingUnsupportedException(PrimitiveComparator<?> comparator, Class<?> exclude) {
    if (Integer.TYPE != exclude) {
      try {
        comparator.compare(0, 0);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (Long.TYPE != exclude) {
      try {
        comparator.compare(0L, 0L);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (Float.TYPE != exclude) {
      try {
        comparator.compare(0.0F, 0.0F);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (Double.TYPE != exclude) {
      try {
        comparator.compare(0.0, 0.0);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (Boolean.TYPE != exclude) {
      try {
        comparator.compare(false, false);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
  }
}
