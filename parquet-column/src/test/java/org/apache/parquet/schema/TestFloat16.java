/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestFloat16 {
  // Smallest negative value a half-precision float may have.
  private static final short LOWEST_VALUE = (short) 0xfbff;
  // Maximum positive finite value a half-precision float may have.
  private static final short MAX_VALUE = (short) 0x7bff;
  // Smallest positive non-zero value a half-precision float may have.
  private static final short MIN_VALUE = (short) 0x0001;
  // Positive 0 of type half-precision float.
  private static final short POSITIVE_ZERO = (short) 0x0000;
  // Negative 0 of type half-precision float.
  private static final short NEGATIVE_ZERO = (short) 0x8000;
  // A Not-a-Number representation of a half-precision float.
  private static final short NaN = (short) 0x7e00;
  // Positive infinity of type half-precision float.
  private static final short POSITIVE_INFINITY = (short) 0x7c00;
  // Negative infinity of type half-precision float.
  private static final short NEGATIVE_INFINITY = (short) 0xfc00;

  @Test
  public void testFloat16ToFloat() {
    // Zeroes
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {0x00, 0x00})))
        .isCloseTo(0.0f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80})))
        .isCloseTo(-0.0f, offset(0.0f));
    // NaN
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xc0, (byte) 0x7f})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7e})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7f})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfe})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xff})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x7f, (byte) 0x7e})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x7f, (byte) 0xfe})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xfe})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7f})))
        .isCloseTo(Float.NaN, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xff})))
        .isCloseTo(Float.NaN, offset(0.0f));
    // infinities
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})))
        .isCloseTo(Float.POSITIVE_INFINITY, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc})))
        .isCloseTo(Float.NEGATIVE_INFINITY, offset(0.0f));
    // subnormals
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00})))
        .isCloseTo(5.9604645E-8f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xfb})))
        .isCloseTo(-65504.0f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b})))
        .isCloseTo(+65504.0f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x83})))
        .isCloseTo(-6.097555E-5f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x80})))
        .isCloseTo(-5.9604645E-8f, offset(0.0f));
    // Known values
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x3c})))
        .isCloseTo(1.0009765625f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0})))
        .isCloseTo(-2.0f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x04})))
        .isCloseTo(6.1035156e-5f, offset(0.0f)); // Inexact
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b})))
        .isCloseTo(65504.0f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x55, (byte) 0x35})))
        .isCloseTo(0.33325195f, offset(0.0f)); // Inexact
    // Denormals (flushed to +/-0)
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x03})))
        .isCloseTo(6.097555e-5f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00})))
        .isCloseTo(5.9604645e-8f, offset(0.0f)); // Inexact
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x83})))
        .isCloseTo(-6.097555e-5f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x80})))
        .isCloseTo(-5.9604645e-8f, offset(0.0f)); // Inexact
    // Miscellaneous values. In general, they're chosen to test the sign/exponent and
    // exponent/mantissa boundaries
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x10})))
        .isCloseTo(+0.00050163269043f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x90})))
        .isCloseTo(-0.00050163269043f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1d, (byte) 0x10})))
        .isCloseTo(+0.000502109527588f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1d, (byte) 0x90})))
        .isCloseTo(-0.000502109527588f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x12})))
        .isCloseTo(+0.00074577331543f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x92})))
        .isCloseTo(-0.00074577331543f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x14})))
        .isCloseTo(+0.00100326538086f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x94})))
        .isCloseTo(-0.00100326538086f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x50})))
        .isCloseTo(+32.875f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0xd0})))
        .isCloseTo(-32.875f, offset(0.0f));
    // A few subnormals for good measure
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x00})))
        .isCloseTo(+1.66893005371e-06f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x80})))
        .isCloseTo(-1.66893005371e-06f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x02})))
        .isCloseTo(+3.21865081787e-05f, offset(0.0f));
    assertThat(Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x82})))
        .isCloseTo(-3.21865081787e-05f, offset(0.0f));
  }

  @Test
  public void testFloatToFloat16() {
    // Zeroes, NaN and infinities
    assertThat(Float16.toFloat16(0.0f)).isEqualTo(POSITIVE_ZERO);
    assertThat(Float16.toFloat16(-0.0f)).isEqualTo(NEGATIVE_ZERO);
    assertThat(Float16.toFloat16(Float.NaN)).isEqualTo(NaN);
    assertThat(Float16.toFloat16(Float.POSITIVE_INFINITY)).isEqualTo(POSITIVE_INFINITY);
    assertThat(Float16.toFloat16(Float.NEGATIVE_INFINITY)).isEqualTo(NEGATIVE_INFINITY);
    // Known values
    assertThat(Float16.toFloat16(1.0009765625f)).isEqualTo((short) 0x3c01);
    assertThat(Float16.toFloat16(-2.0f)).isEqualTo((short) 0xc000);
    assertThat(Float16.toFloat16(6.10352e-5f)).isEqualTo((short) 0x0400);
    assertThat(Float16.toFloat16(65504.0f)).isEqualTo((short) 0x7bff);
    assertThat(Float16.toFloat16(1.0f / 3.0f)).isEqualTo((short) 0x3555);
    // Subnormals
    assertThat(Float16.toFloat16(6.09756e-5f)).isEqualTo((short) 0x03ff);
    assertThat(Float16.toFloat16(5.96046e-8f)).isEqualTo(MIN_VALUE);
    assertThat(Float16.toFloat16(-6.09756e-5f)).isEqualTo((short) 0x83ff);
    assertThat(Float16.toFloat16(-5.96046e-8f)).isEqualTo((short) 0x8001);
    // Subnormals (flushed to +/-0)
    assertThat(Float16.toFloat16(5.96046e-9f)).isEqualTo(POSITIVE_ZERO);
    assertThat(Float16.toFloat16(-5.96046e-9f)).isEqualTo(NEGATIVE_ZERO);
    // Test for values that overflow the mantissa bits into exp bits
    assertThat(Float16.toFloat16(Float.intBitsToFloat(0x39fff000))).isEqualTo((short) 0x1000);
    assertThat(Float16.toFloat16(Float.intBitsToFloat(0x387fe000))).isEqualTo((short) 0x0400);
    // Floats with absolute value above +/-65519 are rounded to +/-inf
    // when using round-to-even
    assertThat(Float16.toFloat16(65519.0f)).isEqualTo((short) 0x7bff);
    assertThat(Float16.toFloat16(65519.9f)).isEqualTo((short) 0x7bff);
    assertThat(Float16.toFloat16(65520.0f)).isEqualTo(POSITIVE_INFINITY);
    assertThat(Float16.toFloat16(-65520.0f)).isEqualTo(NEGATIVE_INFINITY);
    // Check if numbers are rounded to nearest even when they
    // cannot be accurately represented by Half
    assertThat(Float16.toFloat16(2049.0f)).isEqualTo((short) 0x6800);
    assertThat(Float16.toFloat16(4098.0f)).isEqualTo((short) 0x6c00);
    assertThat(Float16.toFloat16(8196.0f)).isEqualTo((short) 0x7000);
    assertThat(Float16.toFloat16(16392.0f)).isEqualTo((short) 0x7400);
    assertThat(Float16.toFloat16(32784.0f)).isEqualTo((short) 0x7800);
    // Miscellaneous values. In general, they're chosen to test the sign/exponent and
    // exponent/mantissa boundaries
    assertThat(Float16.toFloat16(+0.00050163269043f)).isEqualTo((short) 0x101c);
    assertThat(Float16.toFloat16(-0.00050163269043f)).isEqualTo((short) 0x901c);
    assertThat(Float16.toFloat16(+0.000502109527588f)).isEqualTo((short) 0x101d);
    assertThat(Float16.toFloat16(-0.000502109527588f)).isEqualTo((short) 0x901d);
    assertThat(Float16.toFloat16(+0.00074577331543f)).isEqualTo((short) 0x121c);
    assertThat(Float16.toFloat16(-0.00074577331543f)).isEqualTo((short) 0x921c);
    assertThat(Float16.toFloat16(+0.00100326538086f)).isEqualTo((short) 0x141c);
    assertThat(Float16.toFloat16(-0.00100326538086f)).isEqualTo((short) 0x941c);
    assertThat(Float16.toFloat16(+32.875f)).isEqualTo((short) 0x501c);
    assertThat(Float16.toFloat16(-32.875f)).isEqualTo((short) 0xd01c);
    // A few subnormals for good measure
    assertThat(Float16.toFloat16(+1.66893005371e-06f)).isEqualTo((short) 0x001c);
    assertThat(Float16.toFloat16(-1.66893005371e-06f)).isEqualTo((short) 0x801c);
    assertThat(Float16.toFloat16(+3.21865081787e-05f)).isEqualTo((short) 0x021c);
    assertThat(Float16.toFloat16(-3.21865081787e-05f)).isEqualTo((short) 0x821c);
  }

  @Test
  public void testIsNaN() {
    assertThat(Float16.isNaN(POSITIVE_INFINITY)).isFalse();
    assertThat(Float16.isNaN(NEGATIVE_INFINITY)).isFalse();
    assertThat(Float16.isNaN(POSITIVE_ZERO)).isFalse();
    assertThat(Float16.isNaN(NEGATIVE_ZERO)).isFalse();
    assertThat(Float16.isNaN(NaN)).isTrue();
    assertThat(Float16.isNaN((short) 0x7c01)).isTrue();
    assertThat(Float16.isNaN((short) 0x7c18)).isTrue();
    assertThat(Float16.isNaN((short) 0xfc01)).isTrue();
    assertThat(Float16.isNaN((short) 0xfc98)).isTrue();
    assertThat(Float16.isNaN(MAX_VALUE)).isFalse();
    assertThat(Float16.isNaN(LOWEST_VALUE)).isFalse();
    assertThat(Float16.isNaN(Float16.toFloat16(-128.3f))).isFalse();
    assertThat(Float16.isNaN(Float16.toFloat16(128.3f))).isFalse();
  }

  @Test
  public void testCompare() {
    assertThat(Float16.compare(NaN, NaN)).isZero();
    assertThat(Float16.compare(NaN, (short) 0xfc98)).isZero();
    assertThat(Float16.compare(NaN, POSITIVE_INFINITY)).isPositive();
    assertThat(Float16.compare(POSITIVE_INFINITY, NaN)).isNegative();
    assertThat(Float16.compare(POSITIVE_INFINITY, POSITIVE_INFINITY)).isZero();
    assertThat(Float16.compare(NEGATIVE_INFINITY, NEGATIVE_INFINITY)).isZero();
    assertThat(Float16.compare(POSITIVE_INFINITY, NEGATIVE_INFINITY)).isPositive();
    assertThat(Float16.compare(NEGATIVE_INFINITY, POSITIVE_INFINITY)).isNegative();
    assertThat(Float16.compare(POSITIVE_ZERO, POSITIVE_ZERO)).isZero();
    assertThat(Float16.compare(NEGATIVE_ZERO, NEGATIVE_ZERO)).isZero();
    assertThat(Float16.compare(POSITIVE_ZERO, NEGATIVE_ZERO)).isPositive();
    assertThat(Float16.compare(NEGATIVE_ZERO, POSITIVE_ZERO)).isNegative();
    short twelve = Float16.toFloat16(12.462f);
    short minusTwelve = Float16.toFloat16(-12.462f);
    assertThat(Float16.compare(twelve, twelve)).isZero();
    assertThat(Float16.compare(minusTwelve, minusTwelve)).isZero();
    assertThat(Float16.compare(twelve, minusTwelve)).isPositive();
    assertThat(Float16.compare(minusTwelve, twelve)).isNegative();
  }
}
