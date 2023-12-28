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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    assertEquals(0.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {0x00, 0x00})), 0.0f);
    assertEquals(-0.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80})), 0.0f);
    // NaN
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xc0, (byte) 0x7f})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7e})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7f})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfe})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xff})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x7f, (byte) 0x7e})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x7f, (byte) 0xfe})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xfe})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7f})), 0.0f);
    assertEquals(
        Float.NaN, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xff})), 0.0f);
    // infinities
    assertEquals(
        Float.POSITIVE_INFINITY,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})),
        0.0f);
    assertEquals(
        Float.NEGATIVE_INFINITY,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc})),
        0.0f);
    // subnormals
    assertEquals(
        5.9604645E-8f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00})),
        0.0f);
    assertEquals(
        -65504.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xfb})), 0.0f);
    assertEquals(
        +65504.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b})), 0.0f);
    assertEquals(
        -6.097555E-5f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x83})),
        0.0f);
    assertEquals(
        -5.9604645E-8f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x80})),
        0.0f);
    // Known values
    assertEquals(
        1.0009765625f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x3c})),
        0.0f);
    assertEquals(-2.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0})), 0.0f);
    assertEquals(
        6.1035156e-5f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x04})),
        0.0f); // Inexact
    assertEquals(
        65504.0f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b})), 0.0f);
    assertEquals(
        0.33325195f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x55, (byte) 0x35})),
        0.0f); // Inexact
    // Denormals (flushed to +/-0)
    assertEquals(
        6.097555e-5f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x03})),
        0.0f);
    assertEquals(
        5.9604645e-8f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00})),
        0.0f); // Inexact
    assertEquals(
        -6.097555e-5f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x83})),
        0.0f);
    assertEquals(
        -5.9604645e-8f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x80})),
        0.0f); // Inexact
    // Miscellaneous values. In general, they're chosen to test the sign/exponent and
    // exponent/mantissa boundaries
    assertEquals(
        +0.00050163269043f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x10})),
        0.0f);
    assertEquals(
        -0.00050163269043f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x90})),
        0.0f);
    assertEquals(
        +0.000502109527588f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1d, (byte) 0x10})),
        0.0f);
    assertEquals(
        -0.000502109527588f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1d, (byte) 0x90})),
        0.0f);
    assertEquals(
        +0.00074577331543f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x12})),
        0.0f);
    assertEquals(
        -0.00074577331543f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x92})),
        0.0f);
    assertEquals(
        +0.00100326538086f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x14})),
        0.0f);
    assertEquals(
        -0.00100326538086f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x94})),
        0.0f);
    assertEquals(
        +32.875f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x50})), 0.0f);
    assertEquals(
        -32.875f, Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0xd0})), 0.0f);
    // A few subnormals for good measure
    assertEquals(
        +1.66893005371e-06f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x00})),
        0.0f);
    assertEquals(
        -1.66893005371e-06f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x80})),
        0.0f);
    assertEquals(
        +3.21865081787e-05f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x02})),
        0.0f);
    assertEquals(
        -3.21865081787e-05f,
        Float16.toFloat(Binary.fromConstantByteArray(new byte[] {(byte) 0x1c, (byte) 0x82})),
        0.0f);
  }

  @Test
  public void testFloatToFloat16() {
    // Zeroes, NaN and infinities
    assertEquals(POSITIVE_ZERO, Float16.toFloat16(0.0f));
    assertEquals(NEGATIVE_ZERO, Float16.toFloat16(-0.0f));
    assertEquals(NaN, Float16.toFloat16(Float.NaN));
    assertEquals(POSITIVE_INFINITY, Float16.toFloat16(Float.POSITIVE_INFINITY));
    assertEquals(NEGATIVE_INFINITY, Float16.toFloat16(Float.NEGATIVE_INFINITY));
    // Known values
    assertEquals((short) 0x3c01, Float16.toFloat16(1.0009765625f));
    assertEquals((short) 0xc000, Float16.toFloat16(-2.0f));
    assertEquals((short) 0x0400, Float16.toFloat16(6.10352e-5f));
    assertEquals((short) 0x7bff, Float16.toFloat16(65504.0f));
    assertEquals((short) 0x3555, Float16.toFloat16(1.0f / 3.0f));
    // Subnormals
    assertEquals((short) 0x03ff, Float16.toFloat16(6.09756e-5f));
    assertEquals(MIN_VALUE, Float16.toFloat16(5.96046e-8f));
    assertEquals((short) 0x83ff, Float16.toFloat16(-6.09756e-5f));
    assertEquals((short) 0x8001, Float16.toFloat16(-5.96046e-8f));
    // Subnormals (flushed to +/-0)
    assertEquals(POSITIVE_ZERO, Float16.toFloat16(5.96046e-9f));
    assertEquals(NEGATIVE_ZERO, Float16.toFloat16(-5.96046e-9f));
    // Test for values that overflow the mantissa bits into exp bits
    assertEquals((short) 0x1000, Float16.toFloat16(Float.intBitsToFloat(0x39fff000)));
    assertEquals((short) 0x0400, Float16.toFloat16(Float.intBitsToFloat(0x387fe000)));
    // Floats with absolute value above +/-65519 are rounded to +/-inf
    // when using round-to-even
    assertEquals((short) 0x7bff, Float16.toFloat16(65519.0f));
    assertEquals((short) 0x7bff, Float16.toFloat16(65519.9f));
    assertEquals(POSITIVE_INFINITY, Float16.toFloat16(65520.0f));
    assertEquals(NEGATIVE_INFINITY, Float16.toFloat16(-65520.0f));
    // Check if numbers are rounded to nearest even when they
    // cannot be accurately represented by Half
    assertEquals((short) 0x6800, Float16.toFloat16(2049.0f));
    assertEquals((short) 0x6c00, Float16.toFloat16(4098.0f));
    assertEquals((short) 0x7000, Float16.toFloat16(8196.0f));
    assertEquals((short) 0x7400, Float16.toFloat16(16392.0f));
    assertEquals((short) 0x7800, Float16.toFloat16(32784.0f));
    // Miscellaneous values. In general, they're chosen to test the sign/exponent and
    // exponent/mantissa boundaries
    assertEquals((short) 0x101c, Float16.toFloat16(+0.00050163269043f));
    assertEquals((short) 0x901c, Float16.toFloat16(-0.00050163269043f));
    assertEquals((short) 0x101d, Float16.toFloat16(+0.000502109527588f));
    assertEquals((short) 0x901d, Float16.toFloat16(-0.000502109527588f));
    assertEquals((short) 0x121c, Float16.toFloat16(+0.00074577331543f));
    assertEquals((short) 0x921c, Float16.toFloat16(-0.00074577331543f));
    assertEquals((short) 0x141c, Float16.toFloat16(+0.00100326538086f));
    assertEquals((short) 0x941c, Float16.toFloat16(-0.00100326538086f));
    assertEquals((short) 0x501c, Float16.toFloat16(+32.875f));
    assertEquals((short) 0xd01c, Float16.toFloat16(-32.875f));
    // A few subnormals for good measure
    assertEquals((short) 0x001c, Float16.toFloat16(+1.66893005371e-06f));
    assertEquals((short) 0x801c, Float16.toFloat16(-1.66893005371e-06f));
    assertEquals((short) 0x021c, Float16.toFloat16(+3.21865081787e-05f));
    assertEquals((short) 0x821c, Float16.toFloat16(-3.21865081787e-05f));
  }

  @Test
  public void testIsNaN() {
    assertFalse(Float16.isNaN(POSITIVE_INFINITY));
    assertFalse(Float16.isNaN(NEGATIVE_INFINITY));
    assertFalse(Float16.isNaN(POSITIVE_ZERO));
    assertFalse(Float16.isNaN(NEGATIVE_ZERO));
    assertTrue(Float16.isNaN(NaN));
    assertTrue(Float16.isNaN((short) 0x7c01));
    assertTrue(Float16.isNaN((short) 0x7c18));
    assertTrue(Float16.isNaN((short) 0xfc01));
    assertTrue(Float16.isNaN((short) 0xfc98));
    assertFalse(Float16.isNaN(MAX_VALUE));
    assertFalse(Float16.isNaN(LOWEST_VALUE));
    assertFalse(Float16.isNaN(Float16.toFloat16(-128.3f)));
    assertFalse(Float16.isNaN(Float16.toFloat16(128.3f)));
  }

  @Test
  public void testCompare() {
    assertEquals(0, Float16.compare(NaN, NaN));
    assertEquals(0, Float16.compare(NaN, (short) 0xfc98));
    assertEquals(1, Float16.compare(NaN, POSITIVE_INFINITY));
    assertEquals(-1, Float16.compare(POSITIVE_INFINITY, NaN));
    assertEquals(0, Float16.compare(POSITIVE_INFINITY, POSITIVE_INFINITY));
    assertEquals(0, Float16.compare(NEGATIVE_INFINITY, NEGATIVE_INFINITY));
    assertEquals(1, Float16.compare(POSITIVE_INFINITY, NEGATIVE_INFINITY));
    assertEquals(-1, Float16.compare(NEGATIVE_INFINITY, POSITIVE_INFINITY));
    assertEquals(0, Float16.compare(POSITIVE_ZERO, POSITIVE_ZERO));
    assertEquals(0, Float16.compare(NEGATIVE_ZERO, NEGATIVE_ZERO));
    assertEquals(1, Float16.compare(POSITIVE_ZERO, NEGATIVE_ZERO));
    assertEquals(-1, Float16.compare(NEGATIVE_ZERO, POSITIVE_ZERO));
    assertEquals(0, Float16.compare(Float16.toFloat16(12.462f), Float16.toFloat16(12.462f)));
    assertEquals(0, Float16.compare(Float16.toFloat16(-12.462f), Float16.toFloat16(-12.462f)));
    assertEquals(1, Float16.compare(Float16.toFloat16(12.462f), Float16.toFloat16(-12.462f)));
    assertEquals(-1, Float16.compare(Float16.toFloat16(-12.462f), Float16.toFloat16(12.462f)));
  }
}
