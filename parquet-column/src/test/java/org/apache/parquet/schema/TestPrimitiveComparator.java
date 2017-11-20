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

import static org.apache.parquet.schema.PrimitiveComparator.BOOLEAN_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.DOUBLE_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.FLOAT_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.LEXICOGRAPHICAL_BINARY_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_BINARY_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT64_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT64_COMPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPrimitiveComparator {

  @Test
  public void testBooleanComparator() {
    boolean[] values = {false, true};

    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        boolean vi = values[i];
        boolean vj = values[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, BOOLEAN_COMPARATOR.compare(vi, vj));
        assertSignumEquals(vi, vj, exp, BOOLEAN_COMPARATOR.compare(Boolean.valueOf(vi), Boolean.valueOf(vj)));
      }
    }

    checkThrowingUnsupportedException(BOOLEAN_COMPARATOR, Boolean.TYPE);
  }

  @Test
  public void testSignedInt32Comparator() {
    testInt32Comparator(SIGNED_INT32_COMPARATOR,
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
      0,
      1,
      12345,
      Integer.MAX_VALUE,
      Integer.MIN_VALUE,
      -12345,
      -1);
  }

  private void testInt32Comparator(PrimitiveComparator<Integer> comparator, int... values) {
    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        int vi = values[i];
        int vj = values[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, comparator.compare(vi, vj));
        assertSignumEquals(vi, vj, exp, comparator.compare(Integer.valueOf(vi), Integer.valueOf(vj)));
      }
    }

    checkThrowingUnsupportedException(comparator, Integer.TYPE);
  }

  @Test
  public void testSignedInt64Comparator() {
    testInt64Comparator(SIGNED_INT64_COMPARATOR,
      Long.MIN_VALUE,
      -12345678901L,
      -1,
      0,
      1,
      12345678901L,
      Long.MAX_VALUE);
  }

  @Test
  public void testUnsignedInt64Comparator() {
    testInt64Comparator(UNSIGNED_INT64_COMPARATOR,
      0,
      1,
      12345678901L,
      Long.MAX_VALUE,
      Long.MIN_VALUE,
      -12345678901L,
      -1);
  }

  private void testInt64Comparator(PrimitiveComparator<Long> comparator, long... values) {
    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        long vi = values[i];
        long vj = values[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, comparator.compare(vi, vj));
        assertSignumEquals(vi, vj, exp, comparator.compare(Long.valueOf(vi), Long.valueOf(vj)));
      }
    }

    checkThrowingUnsupportedException(comparator, Long.TYPE);
  }

  @Test
  public void testFloatComparator() {
    float[] values = {
      Float.NEGATIVE_INFINITY,
      -Float.MAX_VALUE,
      -1234.5678F,
      -Float.MIN_VALUE,
      0,
      Float.MIN_VALUE,
      1234.5678F,
      Float.MAX_VALUE,
      Float.POSITIVE_INFINITY};

    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        float vi = values[i];
        float vj = values[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, FLOAT_COMPARATOR.compare(vi, vj));
        assertSignumEquals(vi, vj, exp, FLOAT_COMPARATOR.compare(Float.valueOf(vi), Float.valueOf(vj)));
      }
    }

    checkThrowingUnsupportedException(FLOAT_COMPARATOR, Float.TYPE);
  }

  @Test
  public void testDoubleComparator() {
    double[] values = {
      Double.NEGATIVE_INFINITY,
      -Double.MAX_VALUE,
      -123456.7890123456789,
      -Double.MIN_VALUE,
      0,
      Double.MIN_VALUE,
      123456.7890123456789,
      Double.MAX_VALUE,
      Double.POSITIVE_INFINITY};

    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        double vi = values[i];
        double vj = values[j];
        int exp = i - j;
        assertSignumEquals(vi, vj, exp, DOUBLE_COMPARATOR.compare(vi, vj));
        assertSignumEquals(vi, vj, exp, DOUBLE_COMPARATOR.compare(Double.valueOf(vi), Double.valueOf(vj)));
      }
    }

    checkThrowingUnsupportedException(DOUBLE_COMPARATOR, Double.TYPE);
  }

  @Test
  public void testLexicographicalBinaryComparator() {
    testObjectComparator(LEXICOGRAPHICAL_BINARY_COMPARATOR,
      Binary.fromConstantByteArray(new byte[0]),
      Binary.fromConstantByteArray(new byte[]{127, 127, 0, 127}, 2, 1),
      Binary.fromCharSequence("aaa"),
      Binary.fromString("aaaa"),
      Binary.fromReusedByteArray("aaab".getBytes()),
      Binary.fromReusedByteArray("azzza".getBytes(), 1, 3),
      Binary.fromReusedByteBuffer(ByteBuffer.wrap("zzzzzz".getBytes())),
      Binary.fromReusedByteBuffer(ByteBuffer.wrap("aazzzzzzaa".getBytes(), 2, 7)),
      Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[]{-128, -128, -128})),
      Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[]{-128, -128, -1}, 1, 2))
    );
  }

  @Test
  public void testSignedBinaryComparator() {
    testObjectComparator(SIGNED_BINARY_COMPARATOR,
      Binary.fromConstantByteArray(new BigInteger("-9999999999999999999999999999999999999999").toByteArray()),
      Binary.fromReusedByteArray(new BigInteger("-9999999999999999999999999999999999999998").toByteArray()),
      Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toByteArray()),
      Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).toByteArray()),
      Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE).toByteArray()),
      Binary.fromReusedByteArray(new BigInteger("-1").toByteArray()),
      Binary.fromConstantByteBuffer(ByteBuffer.wrap(new BigInteger("0").toByteArray())),
      Binary.fromReusedByteBuffer(ByteBuffer.wrap(new BigInteger("1").toByteArray())),
      Binary.fromConstantByteBuffer(
        ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE).toByteArray())),
      Binary.fromConstantByteBuffer(ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).toByteArray())),
      Binary
        .fromConstantByteBuffer(ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toByteArray())),
      Binary.fromConstantByteBuffer(
        ByteBuffer.wrap(new BigInteger("999999999999999999999999999999999999999").toByteArray())),
      Binary.fromReusedByteBuffer(
        ByteBuffer.wrap(new BigInteger("9999999999999999999999999999999999999998").toByteArray())),
      Binary.fromConstantByteBuffer(
        ByteBuffer.wrap(new BigInteger("9999999999999999999999999999999999999999").toByteArray()))
    );
  }

  private <T> void testObjectComparator(PrimitiveComparator<T> comparator, T... values) {
    for (int i = 0; i < values.length; ++i) {
      for (int j = 0; j < values.length; ++j) {
        T vi = values[i];
        T vj = values[j];
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
        comparator.compare(0F, 0F);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (Double.TYPE != exclude) {
      try {
        comparator.compare(0D, 0D);
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
