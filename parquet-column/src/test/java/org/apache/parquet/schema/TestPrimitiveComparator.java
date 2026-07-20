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

import static org.apache.parquet.schema.PrimitiveComparator.BINARY_AS_FLOAT16_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.BINARY_AS_FLOAT16_IEEE_754_TOTAL_ORDER_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.BINARY_AS_INT96_TIMESTAMP_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.BOOLEAN_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.DOUBLE_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.DOUBLE_IEEE_754_TOTAL_ORDER_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.FLOAT_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.FLOAT_IEEE_754_TOTAL_ORDER_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.SIGNED_INT64_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT32_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_INT64_COMPARATOR;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.parquet.TestUtils;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

/*
 * This test verifies all the PrimitiveComparator implementations. The logic of all tests is the same: list the
 * elements to be tested in ascending order and then compare every elements to each other (including the element
 * itself) and expect the related value based on the defined order.
 */
public class TestPrimitiveComparator {

  @Test
  public void testBooleanComparator() {
    Boolean[] valuesInAscendingOrder = {null, false, true};

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Boolean vi = valuesInAscendingOrder[i];
        Boolean vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, BOOLEAN_COMPARATOR);
        if (vi != null && vj != null) {
          assertOrdering(
              vi, vj, exp, (a, b) -> BOOLEAN_COMPARATOR.compare(a.booleanValue(), b.booleanValue()));
        }
      }
    }

    checkThrowingUnsupportedException(BOOLEAN_COMPARATOR, Boolean.TYPE);
  }

  @Test
  public void testSignedInt32Comparator() {
    testInt32Comparator(
        SIGNED_INT32_COMPARATOR, null, Integer.MIN_VALUE, -12345, -1, 0, 1, 12345, Integer.MAX_VALUE);
  }

  @Test
  public void testUnsignedInt32Comparator() {
    testInt32Comparator(
        UNSIGNED_INT32_COMPARATOR,
        null,
        0, // 0x00000000
        1, // 0x00000001
        12345, // 0x00003039
        Integer.MAX_VALUE, // 0x7FFFFFFF
        Integer.MIN_VALUE, // 0x80000000
        -12345, // 0xFFFFCFC7
        -1); // 0xFFFFFFFF
  }

  private void testInt32Comparator(PrimitiveComparator<Integer> comparator, Integer... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Integer vi = valuesInAscendingOrder[i];
        Integer vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, comparator);
        if (vi != null && vj != null) {
          assertOrdering(vi, vj, exp, (a, b) -> comparator.compare(a.intValue(), b.intValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Integer.TYPE);
  }

  @Test
  public void testUnknownLogicalTypeComparator() {
    PrimitiveType.PrimitiveTypeName[] types = new PrimitiveType.PrimitiveTypeName[] {
      PrimitiveType.PrimitiveTypeName.BOOLEAN,
      PrimitiveType.PrimitiveTypeName.BINARY,
      PrimitiveType.PrimitiveTypeName.INT32,
      PrimitiveType.PrimitiveTypeName.INT64,
      PrimitiveType.PrimitiveTypeName.FLOAT,
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      PrimitiveType.PrimitiveTypeName.INT96,
      PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
    };

    for (PrimitiveType.PrimitiveTypeName type : types) {
      Comparator<Binary> comparator = new PrimitiveType(Type.Repetition.REQUIRED, type, "vo")
          .withLogicalTypeAnnotation(LogicalTypeAnnotation.unknownType())
          .comparator();
      assertThat(comparator.compare(null, null)).isZero();
    }
  }

  @Test
  public void testSignedInt64Comparator() {
    testInt64Comparator(
        SIGNED_INT64_COMPARATOR,
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
    testInt64Comparator(
        UNSIGNED_INT64_COMPARATOR,
        null,
        0L, // 0x0000000000000000
        1L, // 0x0000000000000001
        12345678901L, // 0x00000002DFDC1C35
        Long.MAX_VALUE, // 0x7FFFFFFFFFFFFFFF
        Long.MIN_VALUE, // 0x8000000000000000
        -12345678901L, // 0xFFFFFFFD2023E3CB
        -1L); // 0xFFFFFFFFFFFFFFFF
  }

  private void testInt64Comparator(PrimitiveComparator<Long> comparator, Long... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Long vi = valuesInAscendingOrder[i];
        Long vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, comparator);
        if (vi != null && vj != null) {
          assertOrdering(vi, vj, exp, (a, b) -> comparator.compare(a.longValue(), b.longValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Long.TYPE);
  }

  private void testFloatComparator(PrimitiveComparator<Float> comparator, Float... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Float vi = valuesInAscendingOrder[i];
        Float vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, comparator);
        if (vi != null && vj != null) {
          assertOrdering(vi, vj, exp, (a, b) -> comparator.compare(a.floatValue(), b.floatValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Float.TYPE);
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
      Float.POSITIVE_INFINITY
    };

    testFloatComparator(FLOAT_COMPARATOR, valuesInAscendingOrder);
  }

  @Test
  public void testFloatIEEE754TotalOrderComparator() {
    Float[] valuesInAscendingOrder = {
      null,
      Float.intBitsToFloat(0xFFFFFFFF), // -NaN (smallest)
      Float.intBitsToFloat(0xFFF00001), // -NaN (largest)
      Float.NEGATIVE_INFINITY,
      -Float.MAX_VALUE,
      -1234.5678F,
      -Float.MIN_VALUE,
      -0.0F,
      0.0F,
      Float.MIN_VALUE,
      1234.5678F,
      Float.MAX_VALUE,
      Float.POSITIVE_INFINITY,
      Float.intBitsToFloat(0x7FF00001), // +NaN (smallest)
      Float.intBitsToFloat(0x7FFFFFFF), // +NaN (largest)
    };

    testFloatComparator(FLOAT_IEEE_754_TOTAL_ORDER_COMPARATOR, valuesInAscendingOrder);
  }

  private void testDoubleComparator(PrimitiveComparator<Double> comparator, Double... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Double vi = valuesInAscendingOrder[i];
        Double vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, comparator);
        if (vi != null && vj != null) {
          assertOrdering(vi, vj, exp, (a, b) -> comparator.compare(a.doubleValue(), b.doubleValue()));
        }
      }
    }

    checkThrowingUnsupportedException(comparator, Double.TYPE);
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
      Double.POSITIVE_INFINITY
    };

    testDoubleComparator(DOUBLE_COMPARATOR, valuesInAscendingOrder);
  }

  @Test
  public void testDoubleIEEE754TotalOrderComparator() {
    Double[] valuesInAscendingOrder = {
      null,
      Double.longBitsToDouble(0xFFFFFFFFFFFFFFFFL), // -NaN (smallest)
      Double.longBitsToDouble(0xFFF0000000000001L), // -NaN (largest)
      Double.NEGATIVE_INFINITY,
      -Double.MAX_VALUE,
      -123456.7890123456789,
      -Double.MIN_VALUE,
      -0.0,
      +0.0,
      Double.MIN_VALUE,
      123456.7890123456789,
      Double.MAX_VALUE,
      Double.POSITIVE_INFINITY,
      Double.longBitsToDouble(0x7FF0000000000001L), // +NaN (smallest)
      Double.longBitsToDouble(0x7FFFFFFFFFFFFFFFL), // +NaN (largest)
    };

    testDoubleComparator(DOUBLE_IEEE_754_TOTAL_ORDER_COMPARATOR, valuesInAscendingOrder);
  }

  @Test
  public void testLexicographicalBinaryComparator() {
    testObjectComparator(
        UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR,
        null,
        Binary.fromConstantByteArray(new byte[0]), // ||
        Binary.fromConstantByteArray(new byte[] {127, 127, 0, 127}, 2, 1), // |00|
        Binary.fromCharSequence("aaa"), // |61|61|61|
        Binary.fromString("aaaa"), // |61|61|61|61|
        Binary.fromReusedByteArray("aaab".getBytes()), // |61|61|61|62|
        Binary.fromReusedByteArray("azzza".getBytes(), 1, 3), // |7A|7A|7A|
        Binary.fromReusedByteBuffer(ByteBuffer.wrap("zzzzzz".getBytes())), // |7A|7A|7A|7A|7A|7A|
        Binary.fromReusedByteBuffer(ByteBuffer.wrap("aazzzzzzaa".getBytes(), 2, 7)), // |7A|7A|7A|7A|7A|7A|61|
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {-128, -128, -128})), // |80|80|80|
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {-128, -128, -1}, 1, 2)) // |80|FF|
        );
  }

  @Test
  public void testBinaryAsSignedIntegerComparator() {
    testObjectComparator(
        BINARY_AS_SIGNED_INTEGER_COMPARATOR,
        null,
        Binary.fromConstantByteArray(new BigInteger("-9999999999999999999999999999999999999999").toByteArray()),
        Binary.fromReusedByteArray(new BigInteger("-9999999999999999999999999999999999999998").toByteArray()),
        Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE)
            .subtract(BigInteger.ONE)
            .toByteArray()),
        Binary.fromConstantByteArray(BigInteger.valueOf(Long.MIN_VALUE).toByteArray()),
        Binary.fromConstantByteArray(
            BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE).toByteArray()),
        Binary.fromReusedByteArray(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, -2}, 1, 3),
        Binary.fromReusedByteArray(new BigInteger("-1").toByteArray()),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new BigInteger("0").toByteArray())),
        Binary.fromReusedByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, 0, 1})),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, 0, 2}), 2, 2),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE)
            .subtract(BigInteger.ONE)
            .toByteArray())),
        Binary.fromConstantByteBuffer(
            ByteBuffer.wrap(BigInteger.valueOf(Long.MAX_VALUE).toByteArray())),
        Binary.fromConstantByteBuffer(ByteBuffer.wrap(
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toByteArray())),
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
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, -108})));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, -108})));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, 0, -108})));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, -108})));
    valuesToCompare.add(Binary.fromConstantByteBuffer(ByteBuffer.wrap(new byte[] {0, -108})));

    for (Binary v1 : valuesToCompare) {
      for (Binary v2 : valuesToCompare) {
        assertThat(v1)
            .usingComparator(BINARY_AS_SIGNED_INTEGER_COMPARATOR)
            .isEqualTo(v2);
      }
    }
  }

  private static Binary int96(int julianDay, long nanosOfDay) {
    return new NanoTime(julianDay, nanosOfDay).toBinary();
  }

  @Test
  public void testInt96TimestampComparator() {
    Binary[] valuesInAscendingOrder = {
      int96(Integer.MIN_VALUE, 0), // most negative julian day
      int96(-1, 86_399_999_999_999L), // negative julian days sort before day 0
      int96(0, 0), // start of the julian period
      int96(0, 86_399_999_999_999L), // same day, later time of day
      int96(2440000, 123L), // 1968-05-23T00:00:00.000000123, pre-epoch but positive julian day
      int96(2458850, 43_200_000_000_000L), // 2020-01-01T12:00:00
      int96(2458881, 39_600_000_000_000L), // 2020-02-01T11:00:00, later day even though earlier time of day
      int96(2458881, 39_600_000_000_001L), // 2020-02-01T11:00:00.000000001, nanos tie-break
      int96(Integer.MAX_VALUE, 86_399_999_999_999L)
    };

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        assertThat(Integer.signum(BINARY_AS_INT96_TIMESTAMP_COMPARATOR.compare(
                valuesInAscendingOrder[i], valuesInAscendingOrder[j])))
            .as("comparing value " + i + " to value " + j)
            .isEqualTo(Integer.signum(Integer.compare(i, j)));
      }
    }
  }

  @Test
  public void testInt96TimestampComparatorRejectsInvalidNanos() {
    // Same Julian day so the comparator reaches the nanos validation instead of
    // returning early on the day comparison.
    Binary valid = int96(0, 0);
    for (long invalidNanos : new long[] {-1L, Long.MIN_VALUE, 86_400_000_000_001L, Long.MAX_VALUE}) {
      Binary invalid = int96(0, invalidNanos);
      TestUtils.assertThrows(
          "Expected IllegalArgumentException for nanos=" + invalidNanos,
          IllegalArgumentException.class,
          () -> BINARY_AS_INT96_TIMESTAMP_COMPARATOR.compare(valid, invalid));
    }
  }

  @Test
  public void testFloat16Comparator() {
    Binary[] valuesInAscendingOrder = {
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xfc}), // -Infinity
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xc0}), // -2.0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x84}), // -6.109476E-5
      Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
      Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
    }; // Infinity

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Binary bi = valuesInAscendingOrder[i];
        Binary bj = valuesInAscendingOrder[j];
        if (i < j) {
          assertThat(bi).usingComparator(BINARY_AS_FLOAT16_COMPARATOR).isLessThan(bj);
        } else if (i > j) {
          assertThat(bi).usingComparator(BINARY_AS_FLOAT16_COMPARATOR).isGreaterThan(bj);
        } else {
          assertThat(bi).usingComparator(BINARY_AS_FLOAT16_COMPARATOR).isEqualByComparingTo(bj);
        }
      }
    }
  }

  @Test
  public void testBinaryAsFloat16IEEE754TotalOrderComparator() {
    Binary[] valuesInAscendingOrder = {
      null,
      Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xff}), // -NaN (smallest)
      Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0xfc}), // -NaN (largest)
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xfc}), // -Infinity
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xc0}), // -2.0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x84}), // -6.109476E-5
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x80}), // -0
      Binary.fromConstantByteArray(new byte[] {0x00, 0x00}), // +0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
      Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
      Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c}), // Infinity
      Binary.fromConstantByteArray(new byte[] {0x01, 0x7c}), // +NaN (smallest)
      Binary.fromConstantByteArray(new byte[] {(byte) 0xff, 0x7f}) // +NaN (largest)
    };

    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        Binary vi = valuesInAscendingOrder[i];
        Binary vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, BINARY_AS_FLOAT16_IEEE_754_TOTAL_ORDER_COMPARATOR);
      }
    }
  }

  private <T> void testObjectComparator(PrimitiveComparator<T> comparator, T... valuesInAscendingOrder) {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        T vi = valuesInAscendingOrder[i];
        T vj = valuesInAscendingOrder[j];
        int exp = i - j;
        assertOrdering(vi, vj, exp, comparator);
      }
    }

    checkThrowingUnsupportedException(comparator, null);
  }

  private <T> void assertOrdering(T v1, T v2, int expectedSignum, Comparator<T> comparator) {
    int compareResult = comparator.compare(v1, v2);
    if (expectedSignum < 0) {
      assertThat(compareResult).isNegative();
    } else if (expectedSignum > 0) {
      assertThat(compareResult).isPositive();
    } else {
      assertThat(compareResult).isZero();
    }
  }

  private void checkThrowingUnsupportedException(PrimitiveComparator<?> comparator, Class<?> exclude) {
    if (Integer.TYPE != exclude) {
      assertThatThrownBy(() -> comparator.compare(0, 0))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("compare(int, int) was called on a non-int comparator: " + comparator);
    }
    if (Long.TYPE != exclude) {
      assertThatThrownBy(() -> comparator.compare(0L, 0L))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("compare(long, long) was called on a non-long comparator: " + comparator);
    }
    if (Float.TYPE != exclude) {
      assertThatThrownBy(() -> comparator.compare(0.0F, 0.0F))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("compare(float, float) was called on a non-float comparator: " + comparator);
    }
    if (Double.TYPE != exclude) {
      assertThatThrownBy(() -> comparator.compare(0.0, 0.0))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining(
              "compare(double, double) was called on a non-double comparator: " + comparator);
    }
    if (Boolean.TYPE != exclude) {
      assertThatThrownBy(() -> comparator.compare(false, false))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining(
              "compare(boolean, boolean) was called on a non-boolean comparator: " + comparator);
    }
  }
}
